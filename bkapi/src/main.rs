use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use actix_service::Service;
use actix_web::{
    get,
    web::{self, Data},
    App, HttpResponse, HttpServer,
};
use async_nats::service::ServiceExt;
use clap::Parser;
use flate2::{write::GzEncoder, Compression};
use futures::{StreamExt, TryStreamExt};
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing_unwrap::ResultExt;

mod tree;

lazy_static::lazy_static! {
    static ref HTTP_REQUEST_COUNT: prometheus::CounterVec = prometheus::register_counter_vec!("http_requests_total", "Number of HTTP requests", &["http_route", "http_method", "http_status_code"]).unwrap();
    static ref HTTP_REQUEST_DURATION: prometheus::HistogramVec = prometheus::register_histogram_vec!("http_request_duration_seconds", "Duration of HTTP requests", &["http_route", "http_method", "http_status_code"]).unwrap();
}

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("row was unable to be loaded: {0}")]
    LoadingRow(sqlx::Error),
    #[error("listener could not listen: {0}")]
    Listener(sqlx::Error),
    #[error("listener got data that could not be decoded: {0}")]
    Data(serde_json::Error),
    #[error("nats encountered error: {0}")]
    Nats(#[from] NatsError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(thiserror::Error, Debug)]
enum NatsError {
    #[error("{0}")]
    Subscribe(#[from] async_nats::SubscribeError),
    #[error("{0}")]
    CreateStream(#[from] async_nats::jetstream::context::CreateStreamError),
    #[error("{0}")]
    Consumer(#[from] async_nats::jetstream::stream::ConsumerError),
    #[error("{0}")]
    Stream(#[from] async_nats::jetstream::consumer::StreamError),
    #[error("{0}")]
    Request(#[from] async_nats::jetstream::context::RequestError),
    #[error("{0}")]
    Generic(#[from] async_nats::Error),
}

#[derive(Parser, Clone)]
struct Config {
    /// Host to listen for incoming HTTP requests.
    #[clap(long, env, default_value = "127.0.0.1:3000")]
    http_listen: String,

    /// Host to listen for metrics requests.
    #[clap(long, env, default_value = "127.0.0.1:3001")]
    metrics_host: SocketAddr,
    /// If logs should be output in JSON format and sent to otlp collector.
    #[clap(long, env)]
    json_logs: bool,

    /// Database URL for fetching data.
    #[clap(long, env)]
    database_url: String,
    /// Query to perform to fetch initial values.
    #[clap(long, env)]
    database_query: String,

    /// If provided, the Postgres notification topic to subscribe to.
    #[clap(long, env, required_unless_present = "nats_url")]
    database_subscribe: Option<String>,

    /// NATS URLs.
    #[clap(long, env, requires = "nats_prefix")]
    nats_url: Option<String>,
    /// Path to NATS credential file.
    #[clap(long, env)]
    nats_creds: Option<PathBuf>,
    /// Prefix to use for NATS subjects.
    #[clap(long, env)]
    nats_prefix: Option<String>,

    /// Maximum distance permitted in queries.
    #[clap(long, env, default_value = "10")]
    max_distance: u32,
}

#[actix_web::main]
async fn main() {
    let _ = dotenvy::dotenv();

    let config = Config::parse();

    foxlib::trace_init(foxlib::TracingConfig {
        namespace: "bkapi",
        name: "bkapi",
        version: env!("CARGO_PKG_VERSION"),
        otlp: config.json_logs,
    });

    tracing::info!("starting bkapi");

    let token = CancellationToken::new();

    let metrics_server = foxlib::MetricsServer::serve(config.metrics_host, false).await;

    let tree = tree::Tree::new();

    tracing::trace!("connecting to postgres");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&config.database_url)
        .await
        .expect_or_log("could not connect to database");
    tracing::debug!("connected to postgres");

    let (sender, receiver) = futures::channel::oneshot::channel();

    let client = match (config.nats_url.as_deref(), config.nats_creds.as_deref()) {
        (Some(host), None) => Some(
            async_nats::connect(host)
                .await
                .expect_or_log("could not connect to nats with no nkey"),
        ),
        (Some(host), Some(creds_path)) => Some(
            async_nats::ConnectOptions::with_credentials_file(creds_path.to_owned())
                .await
                .expect_or_log("could not open credentials file")
                .custom_inbox_prefix(format!(
                    "_INBOX_{}",
                    config.nats_prefix.as_deref().unwrap().replace('.', "_")
                ))
                .connect(host)
                .await
                .expect_or_log("could not connect to nats with nkey"),
        ),
        _ => None,
    };

    let tree_clone = tree.clone();
    let config_clone = config.clone();
    let token_clone = token.clone();
    let mut listener_task = if let Some(subscription) = config.database_subscribe.clone() {
        tracing::info!("starting to listen for payloads from postgres");
        tokio::spawn(async move {
            tree::listen_for_payloads_db(
                pool,
                subscription,
                config_clone.database_query,
                tree_clone,
                sender,
                token_clone,
            )
            .await
            .expect_or_log("could not listen for payloads")
        })
    } else if let Some(client) = client.clone() {
        tracing::info!("starting to listen for payloads from nats");
        tokio::spawn(async move {
            tree::listen_for_payloads_nats(
                config_clone,
                pool,
                client,
                tree_clone,
                sender,
                token_clone,
            )
            .await
            .expect_or_log("could not listen for payloads")
        })
    } else {
        panic!("no listener source available");
    };

    tracing::info!("waiting for initial tree to load");
    receiver
        .await
        .expect_or_log("tree loading was dropped before completing");

    tracing::info!("initial tree loaded, starting server");
    metrics_server.set_ready(true);

    if let Some(client) = client {
        let tree = tree.clone();
        let config = config.clone();
        let token = token.clone();

        tokio::spawn(async move {
            search_nats(client, tree, config, token)
                .await
                .unwrap_or_log();
        });
    }

    let mut server = start_server(config, tree);
    let server_handle = server.handle();

    tokio::spawn({
        let token = token.clone();

        async move {
            tokio::signal::ctrl_c()
                .await
                .expect_or_log("ctrl+c handler failed to install");
            token.cancel();
        }
    });

    tokio::select! {
        _ = token.cancelled() => {
            tracing::info!("got cancellation, stopping server");
            let _ = tokio::join!(server_handle.stop(true), server, listener_task);
        }
        res = &mut listener_task => {
            tracing::error!("listener task ended: {res:?}");
            let _ = tokio::join!(server_handle.stop(true), server);
        }
        res = &mut server => {
            tracing::error!("server ended: {res:?}");
            let _ = tokio::join!(server_handle.stop(true), listener_task);
        }
    }
}

fn start_server(config: Config, tree: tree::Tree) -> actix_web::dev::Server {
    let tree = Data::new(tree);
    let config_data = Data::new(config.clone());

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
            .app_data(config_data.clone())
            .service(search)
            .service(dump)
    })
    .bind(&config.http_listen)
    .expect_or_log("bind failed")
    .disable_signals()
    .run()
}

#[derive(Debug, serde::Deserialize)]
struct Query {
    hash: i64,
    distance: u32,
}

#[derive(serde::Serialize)]
struct SearchResponse {
    hash: i64,
    distance: u32,

    hashes: Vec<tree::HashDistance>,
}

#[get("/search")]
#[tracing::instrument(skip(query, tree, config), fields(query = ?query.0))]
async fn search(
    query: web::Query<Query>,
    tree: Data<tree::Tree>,
    config: Data<Config>,
) -> HttpResponse {
    let Query { hash, distance } = query.0;
    let distance = distance.clamp(0, config.max_distance);

    let hashes = tree
        .find([tree::HashDistance { hash, distance }])
        .await
        .remove(0);

    let resp = SearchResponse {
        hash,
        distance,
        hashes,
    };

    HttpResponse::Ok().json(resp)
}

#[derive(serde::Deserialize)]
struct SearchPayload {
    hash: i64,
    distance: u32,
}

#[tracing::instrument(skip_all)]
async fn search_nats(
    client: async_nats::Client,
    tree: tree::Tree,
    config: Config,
    token: CancellationToken,
) -> Result<(), Error> {
    tracing::info!("subscribing to searches");

    let client = Arc::new(client);
    let max_distance = config.max_distance;

    let service = client
        .add_service(async_nats::service::Config {
            name: format!(
                "{}-bkapi",
                config.nats_prefix.as_deref().unwrap().replace('.', "-")
            ),
            version: env!("CARGO_PKG_VERSION").to_string(),
            description: None,
            stats_handler: None,
            metadata: None,
            queue_group: None,
        })
        .await
        .map_err(NatsError::Generic)?;

    let mut endpoint = service
        .endpoint(format!("{}.bkapi.search", config.nats_prefix.unwrap()))
        .await
        .map_err(NatsError::Generic)?;

    loop {
        tokio::select! {
            Some(request) = endpoint.next() => {
                tracing::trace!("got search message");

                if let Err(err) = handle_search_nats(max_distance, tree.clone(), request).await {
                    tracing::error!("could not handle nats search: {err}");
                }
            }
            _ = token.cancelled() => {
                tracing::info!("cancelled, stopping endpoint");
                break;
            }
        }
    }

    if let Err(err) = endpoint.stop().await {
        tracing::error!("could not stop endpoint: {err}");
    }

    Ok(())
}

async fn handle_search_nats(
    max_distance: u32,
    tree: tree::Tree,
    request: async_nats::service::Request,
) -> Result<(), Error> {
    let payloads: Vec<SearchPayload> = match serde_json::from_slice(&request.message.payload) {
        Ok(payloads) => payloads,
        Err(err) => {
            let err = Err(async_nats::service::error::Error {
                status: err.to_string(),
                code: 400,
            });

            if let Err(err) = request.respond(err).await {
                tracing::error!("could not respond with error: {err}");
            }

            return Ok(());
        }
    };

    tokio::task::spawn(
        async move {
            let hashes = payloads.into_iter().map(|payload| tree::HashDistance {
                hash: payload.hash,
                distance: payload.distance.clamp(0, max_distance),
            });

            let results = tree.find(hashes).await;

            let resp = serde_json::to_vec(&results).map(Into::into).map_err(|err| {
                async_nats::service::error::Error {
                    status: err.to_string(),
                    code: 503,
                }
            });

            if let Err(err) = request.respond(resp).await {
                tracing::error!("could not respond: {err}");
            }
        }
        .in_current_span(),
    );

    Ok(())
}

#[get("/dump")]
async fn dump(tree: Data<tree::Tree>) -> HttpResponse {
    tracing::info!("starting dump request");
    let (wtr, rdr) = tokio::io::duplex(4096);

    let span = tracing::info_span!("dump_blocking");
    tokio::task::spawn_blocking(move || {
        let _entered = span.entered();

        let tree = tree.tree.blocking_read();

        let bridge = tokio_util::io::SyncIoBridge::new(wtr);
        let mut compressor = GzEncoder::new(bridge, Compression::default());

        if let Err(err) = bincode::serde::encode_into_std_write(
            &*tree,
            &mut compressor,
            bincode::config::standard(),
        ) {
            tracing::error!("could not write tree to compressor: {err}");
        }

        match compressor.finish() {
            Ok(mut file) => match file.shutdown() {
                Ok(_) => tracing::info!("finished writing dump"),
                Err(err) => tracing::error!("could not finish writing dump: {err}"),
            },
            Err(err) => {
                tracing::error!("could not finish compressor: {err}");
            }
        }
    });

    let stream = tokio_util::codec::FramedRead::new(rdr, tokio_util::codec::BytesCodec::new())
        .map_ok(|b| b.freeze());

    HttpResponse::Ok()
        .content_type("application/octet-stream")
        .insert_header((
            "content-disposition",
            r#"attachment; filename="bkapi-dump.dat.gz""#,
        ))
        .streaming(stream)
}
