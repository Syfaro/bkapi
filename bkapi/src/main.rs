use std::sync::Arc;

use actix_service::Service;
use actix_web::{
    get,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder,
};
use envconfig::Envconfig;
use opentelemetry::KeyValue;
use prometheus::{Encoder, TextEncoder};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::RwLock;
use tracing_subscriber::layer::SubscriberExt;
use tracing_unwrap::ResultExt;

mod tree;

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

#[actix_web::main]
async fn main() {
    let config = Config::init_from_env().expect("could not load config");
    configure_tracing(&config);

    tracing::info!("starting bkbase");

    let tree: tree::Tree = Arc::new(RwLock::new(bk_tree::BKTree::new(tree::Hamming)));

    tracing::trace!("connecting to postgres");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&config.database_url)
        .await
        .expect_or_log("could not connect to database");
    tracing::debug!("connected to postgres");

    let http_listen = config.http_listen.clone();

    let (sender, receiver) = futures::channel::oneshot::channel();

    tracing::info!("starting to listen for payloads");
    let tree_clone = tree.clone();
    let config_clone = config.clone();
    tokio::task::spawn(async {
        tree::listen_for_payloads(pool, config_clone, tree_clone, sender)
            .await
            .expect_or_log("listenting for updates failed");
    });

    tracing::info!("waiting for initial tree to load");
    receiver
        .await
        .expect_or_log("tree loading was dropped before completing");
    tracing::info!("initial tree loaded, starting server");

    let tree = Data::new(tree);
    let config = Data::new(config);

    HttpServer::new(move || {
        App::new()
            .wrap(tracing_actix_web::TracingLogger::default())
            .wrap_fn(|req, srv| {
                let path = req.path().to_owned();
                let method = req.method().to_string();

                let start = std::time::Instant::now();
                let fut = srv.call(req);

                async move {
                    let res = fut.await?;
                    let end = std::time::Instant::now().duration_since(start);

                    let status_code = res.status().as_u16().to_string();

                    let labels: Vec<&str> = vec![&path, &method, &status_code];
                    HTTP_REQUEST_COUNT.with_label_values(&labels).inc();
                    HTTP_REQUEST_DURATION
                        .with_label_values(&labels)
                        .observe(end.as_secs_f64());

                    Ok(res)
                }
            })
            .app_data(tree.clone())
            .app_data(config.clone())
            .service(search)
            .service(health)
            .service(metrics)
    })
    .bind(&http_listen)
    .expect_or_log("bind failed")
    .run()
    .await
    .expect_or_log("server failed");
}

fn configure_tracing(config: &Config) {
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
        .install_batch(opentelemetry::runtime::Tokio)
        .expect("otel jaeger pipeline could not be created");

    let trace = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing::subscriber::set_global_default(
        tracing_subscriber::Registry::default()
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(trace)
            .with(tracing_subscriber::fmt::layer()),
    )
    .expect("tracing could not be configured");
}

#[derive(Debug, serde::Deserialize)]
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

#[get("/search")]
#[tracing::instrument(skip(query, tree, config), fields(query = ?query.0))]
async fn search(
    query: web::Query<Query>,
    tree: Data<tree::Tree>,
    config: Data<Config>,
) -> Result<HttpResponse, std::convert::Infallible> {
    let Query { hash, distance } = query.0;
    let max_distance = config.max_distance;

    tracing::info!("searching for hash {} with distance {}", hash, distance);

    if matches!(max_distance, Some(max_distance) if distance > max_distance) {
        return Ok(HttpResponse::BadRequest().body("distance is greater than max distance"));
    }

    let tree = tree.read().await;

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

    Ok(HttpResponse::Ok().json(resp))
}

#[get("/health")]
async fn health() -> impl Responder {
    "OK"
}

#[get("/metrics")]
async fn metrics() -> Result<HttpResponse, std::convert::Infallible> {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();

    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    Ok(HttpResponse::Ok().body(buffer))
}
