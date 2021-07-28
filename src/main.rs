use std::sync::Arc;

use async_std::sync::{RwLock, RwLockUpgradableReadGuard};
use envconfig::Envconfig;
use opentelemetry::KeyValue;
use sqlx::{
    postgres::{PgListener, PgPoolOptions},
    Pool, Postgres, Row,
};
use tide::Request;
use tracing_subscriber::layer::SubscriberExt;
use tracing_unwrap::ResultExt;

mod middlewares;

lazy_static::lazy_static! {
    static ref HTTP_REQUEST_COUNT: prometheus::CounterVec = prometheus::register_counter_vec!("http_requests_total", "Number of HTTP requests", &["http_route", "http_method", "http_status_code"]).unwrap();
    static ref HTTP_REQUEST_DURATION: prometheus::HistogramVec = prometheus::register_histogram_vec!("http_request_duration_seconds", "Duration of HTTP requests", &["http_route", "http_method", "http_status_code"]).unwrap();

    static ref TREE_DURATION: prometheus::HistogramVec = prometheus::register_histogram_vec!("bkapi_tree_duration_seconds", "Duration of tree search time", &["distance"]).unwrap();
    static ref TREE_ADD_DURATION: prometheus::Histogram = prometheus::register_histogram!("bkapi_tree_add_duration_seconds", "Duration to add new item to tree").unwrap();
}

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("row was unable to be loaded: {0}")]
    LoadingRow(sqlx::Error),
    #[error("listener could not listen: {0}")]
    Listener(sqlx::Error),
    #[error("listener got data that could not be decoded: {0}")]
    Data(serde_json::Error),
}

type Tree = Arc<RwLock<bk_tree::BKTree<Node, Hamming>>>;

#[derive(Envconfig, Clone)]
struct Config {
    #[envconfig(default = "0.0.0.0:3000")]
    http_listen: String,
    #[envconfig(default = "127.0.0.1:6831")]
    jaeger_agent: String,
    #[envconfig(default = "bkapi")]
    service_name: String,

    database_url: String,
    database_query: String,
    database_subscribe: String,
    #[envconfig(default = "false")]
    database_is_unique: bool,

    max_distance: Option<u32>,
}

/// A hamming distance metric.
struct Hamming;

impl bk_tree::Metric<Node> for Hamming {
    fn distance(&self, a: &Node, b: &Node) -> u32 {
        hamming::distance_fast(&a.0, &b.0).expect_or_log("hashes did not have same byte alignment")
            as u32
    }

    fn threshold_distance(&self, a: &Node, b: &Node, _threshold: u32) -> Option<u32> {
        Some(self.distance(a, b))
    }
}

/// A value of a node in the BK tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Node([u8; 8]);

impl From<i64> for Node {
    fn from(num: i64) -> Self {
        Self(num.to_be_bytes())
    }
}

impl From<Node> for i64 {
    fn from(node: Node) -> Self {
        i64::from_be_bytes(node.0)
    }
}

#[async_std::main]
async fn main() {
    let config = Config::init_from_env().expect("could not load config");

    opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());

    let env = std::env::var("ENVIRONMENT");
    let env = if let Ok(env) = env.as_ref() {
        env.as_str()
    } else if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };

    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_agent_endpoint(&config.jaeger_agent)
        .with_service_name(&config.service_name)
        .with_tags(vec![
            KeyValue::new("environment", env.to_owned()),
            KeyValue::new("version", env!("CARGO_PKG_VERSION")),
        ])
        .install_batch(opentelemetry::runtime::AsyncStd)
        .expect("otel jaeger pipeline could not be created");

    let trace = tracing_opentelemetry::layer().with_tracer(tracer.clone());
    tracing::subscriber::set_global_default(
        tracing_subscriber::Registry::default()
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(trace)
            .with(tracing_subscriber::fmt::layer()),
    )
    .expect("tracing could not be configured");

    tracing::info!("starting bkbase");

    tracing::debug!("loaded config");

    let tree: Tree = Arc::new(RwLock::new(bk_tree::BKTree::new(Hamming)));

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&config.database_url)
        .await
        .expect_or_log("could not connect to database");
    tracing::debug!("connected to postgres");

    let http_listen = config.http_listen.clone();
    let max_distance = config.max_distance;

    let (sender, receiver) = futures::channel::oneshot::channel();

    tracing::info!("starting to listen for payloads");
    let tree_clone = tree.clone();
    async_std::task::spawn(async {
        listen_for_payloads(pool, config, tree_clone, sender)
            .await
            .expect_or_log("listenting for updates failed");
    });

    tracing::info!("waiting for initial tree to load");
    receiver
        .await
        .expect_or_log("tree loading was dropped before completing");
    tracing::info!("initial tree loaded, starting server");

    let mut app = tide::with_state(State { tree, max_distance });
    app.with(middlewares::TideOpentelemMiddleware::new(tracer));
    app.with(tide_tracing::TraceMiddleware::new());
    app.with(middlewares::TidePrometheusMiddleware);

    app.at("/search").get(search);
    app.at("/health").get(|_| async { Ok("OK") });

    app.listen(&http_listen)
        .await
        .expect_or_log("could not start web server");
}

#[derive(Clone)]
struct State {
    tree: Tree,
    max_distance: Option<u32>,
}

#[derive(serde::Deserialize)]
struct Query {
    hash: i64,
    distance: u32,
}

#[derive(serde::Serialize)]
struct HashDistance {
    hash: i64,
    distance: u32,
}

#[derive(serde::Serialize)]
struct SearchResponse {
    hash: i64,
    distance: u32,

    hashes: Vec<HashDistance>,
}

#[tracing::instrument(skip(req))]
async fn search(req: Request<State>) -> tide::Result {
    let state = req.state();

    let Query { hash, distance } = req.query()?;
    tracing::info!("searching for hash {} with distance {}", hash, distance);

    if matches!(state.max_distance, Some(max_distance) if distance > max_distance) {
        return Err(tide::Error::from_str(
            400,
            "Distance is greater than max distance",
        ));
    }

    let tree = state.tree.read().await;

    let duration = TREE_DURATION
        .with_label_values(&[&distance.to_string()])
        .start_timer();
    let matches: Vec<HashDistance> = tree
        .find(&hash.into(), distance)
        .into_iter()
        .map(|item| HashDistance {
            distance: item.0,
            hash: (*item.1).into(),
        })
        .collect();
    let time = duration.stop_and_record();

    tracing::debug!("found {} items in {} seconds", matches.len(), time);

    let resp = SearchResponse {
        hash,
        distance,
        hashes: matches,
    };

    Ok(serde_json::to_string(&resp)?.into())
}

/// Create a new BK tree and pull in all hashes from provided query.
///
/// This must be called after you have started a listener, otherwise items may
/// be lost.
async fn create_tree(
    conn: &Pool<Postgres>,
    config: &Config,
) -> Result<bk_tree::BKTree<Node, Hamming>, Error> {
    use futures::TryStreamExt;

    tracing::warn!("creating new tree");

    let mut tree = bk_tree::BKTree::new(Hamming);

    let mut rows = sqlx::query(&config.database_query).fetch(conn);

    let mut count = 0;

    let start = std::time::Instant::now();

    while let Some(row) = rows.try_next().await.map_err(Error::LoadingRow)? {
        let node: Node = row.get::<i64, _>(0).into();

        // Avoid checking if each value is unique if we were told that the
        // database query only returns unique values.
        let timer = TREE_ADD_DURATION.start_timer();
        if config.database_is_unique || tree.find_exact(&node).is_none() {
            tree.add(node);
        }
        timer.stop_and_record();

        count += 1;
        if count % 250_000 == 0 {
            tracing::debug!(count, "loaded more rows");
        }
    }

    let dur = std::time::Instant::now().duration_since(start);

    tracing::info!(count, "completed loading rows in {:?}", dur);

    Ok(tree)
}

#[derive(serde::Deserialize)]
struct Payload {
    hash: i64,
}

/// Listen for incoming payloads.
///
/// This will create a new tree to ensure all items are present. It will also
/// automatically recreate trees as needed if the database connection is lost.
async fn listen_for_payloads(
    conn: Pool<Postgres>,
    config: Config,
    tree: Tree,
    initial: futures::channel::oneshot::Sender<()>,
) -> Result<(), Error> {
    let mut listener = PgListener::connect_with(&conn)
        .await
        .map_err(Error::Listener)?;
    listener
        .listen(&config.database_subscribe)
        .await
        .map_err(Error::Listener)?;

    let new_tree = create_tree(&conn, &config).await?;
    {
        let mut tree = tree.write().await;
        *tree = new_tree;
    }

    initial
        .send(())
        .expect_or_log("nothing listening for initial data");

    loop {
        while let Some(notification) = listener.try_recv().await.map_err(Error::Listener)? {
            let payload: Payload =
                serde_json::from_str(notification.payload()).map_err(Error::Data)?;
            tracing::debug!(hash = payload.hash, "evaluating new payload");

            let node: Node = payload.hash.into();

            let _timer = TREE_ADD_DURATION.start_timer();

            let tree = tree.upgradable_read().await;
            if tree.find_exact(&node).is_some() {
                tracing::trace!("hash already existed in tree");
                continue;
            }

            tracing::trace!("hash did not exist, adding to tree");
            let mut tree = RwLockUpgradableReadGuard::upgrade(tree).await;
            tree.add(node);
        }

        tracing::error!("disconnected from listener, recreating tree");
        async_std::task::sleep(std::time::Duration::from_secs(10)).await;
        let new_tree = create_tree(&conn, &config).await?;
        {
            let mut tree = tree.write().await;
            *tree = new_tree;
        }
    }
}
