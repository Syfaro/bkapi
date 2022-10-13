use std::sync::Arc;

use actix_service::Service;
use actix_web::{
    get,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder,
};
use clap::Parser;
use futures::StreamExt;
use opentelemetry::KeyValue;
use prometheus::{Encoder, TextEncoder};
use sqlx::postgres::PgPoolOptions;
use tracing::Instrument;
use tracing_subscriber::layer::SubscriberExt;
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
    Nats(#[from] async_nats::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Parser, Clone)]
struct Config {
    /// Host to listen for incoming HTTP requests.
    #[clap(long, env, default_value = "127.0.0.1:3000")]
    http_listen: String,

    /// Jaeger agent endpoint for span collection.
    #[clap(long, env, default_value = "127.0.0.1:6831")]
    jaeger_agent: String,
    /// Service name for spans.
    #[clap(long, env, default_value = "bkapi")]
    service_name: String,

    /// Database URL for fetching data.
    #[clap(long, env)]
    database_url: String,
    /// Query to perform to fetch initial values.
    #[clap(long, env)]
    database_query: String,

    /// If provided, the Postgres notification topic to subscribe to.
    #[clap(long, env)]
    database_subscribe: Option<String>,

    /// The NATS host.
    #[clap(long, env)]
    nats_host: Option<String>,
    /// The NATS NKEY.
    #[clap(long, env)]
    nats_nkey: Option<String>,

    /// Maximum distance permitted in queries.
    #[clap(long, env, default_value = "10")]
    max_distance: u32,
}

#[actix_web::main]
async fn main() {
    let _ = dotenvy::dotenv();

    let config = Config::parse();
    configure_tracing(&config);

    tracing::info!("starting bkapi");

    let tree = tree::Tree::new();

    tracing::trace!("connecting to postgres");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&config.database_url)
        .await
        .expect_or_log("could not connect to database");
    tracing::debug!("connected to postgres");

    let (sender, receiver) = futures::channel::oneshot::channel();

    let client = match (config.nats_host.as_deref(), config.nats_nkey.as_deref()) {
        (Some(host), None) => Some(
            async_nats::connect(host)
                .await
                .expect_or_log("could not connect to nats with no nkey"),
        ),
        (Some(host), Some(nkey)) => Some(
            async_nats::ConnectOptions::with_nkey(nkey.to_string())
                .connect(host)
                .await
                .expect_or_log("could not connect to nats with nkey"),
        ),
        _ => None,
    };

    let tree_clone = tree.clone();
    let config_clone = config.clone();
    if let Some(subscription) = config.database_subscribe.clone() {
        tracing::info!("starting to listen for payloads from postgres");

        let query = config.database_query.clone();

        tokio::task::spawn(async move {
            tree::listen_for_payloads_db(pool, subscription, query, tree_clone, sender)
                .await
                .unwrap_or_log();
        });
    } else if let Some(client) = client.clone() {
        tracing::info!("starting to listen for payloads from nats");

        tokio::task::spawn(async {
            tree::listen_for_payloads_nats(config_clone, pool, client, tree_clone, sender)
                .await
                .unwrap_or_log();
        });
    } else {
        panic!("no listener source available");
    };

    tracing::info!("waiting for initial tree to load");
    receiver
        .await
        .expect_or_log("tree loading was dropped before completing");
    tracing::info!("initial tree loaded, starting server");

    if let Some(client) = client {
        let tree_clone = tree.clone();
        let config_clone = config.clone();
        tokio::task::spawn(async move {
            search_nats(client, tree_clone, config_clone)
                .await
                .unwrap_or_log();
        });
    }

    start_server(config, tree).await.unwrap_or_log();
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

    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_endpoint(&config.jaeger_agent)
        .with_service_name(&config.service_name)
        .with_trace_config(opentelemetry::sdk::trace::config().with_resource(
            opentelemetry::sdk::Resource::new(vec![
                KeyValue::new("environment", env.to_owned()),
                KeyValue::new("version", env!("CARGO_PKG_VERSION")),
            ]),
        ))
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

async fn start_server(config: Config, tree: tree::Tree) -> Result<(), Error> {
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
            .service(health)
            .service(metrics)
    })
    .bind(&config.http_listen)
    .expect_or_log("bind failed")
    .run()
    .await
    .map_err(Error::Io)
}

#[get("/health")]
async fn health() -> impl Responder {
    "OK"
}

#[get("/metrics")]
async fn metrics() -> HttpResponse {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();

    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    HttpResponse::Ok().body(buffer)
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

#[tracing::instrument(skip(client, tree, config))]
async fn search_nats(
    client: async_nats::Client,
    tree: tree::Tree,
    config: Config,
) -> Result<(), Error> {
    tracing::info!("subscribing to searches");

    let client = Arc::new(client);

    let mut sub = client
        .queue_subscribe("bkapi.search".to_string(), "bkapi-search".to_string())
        .await?;

    while let Some(message) = sub.next().await {
        tracing::trace!("got search message");

        let reply = match message.reply {
            Some(reply) => reply,
            None => {
                tracing::warn!("message had no reply subject, skipping");
                continue;
            }
        };

        let payloads: Vec<SearchPayload> =
            serde_json::from_slice(&message.payload).map_err(Error::Data)?;

        let tree = tree.clone();
        let client = client.clone();
        let max_distance = config.max_distance;

        tokio::task::spawn(
            async move {
                let hashes = payloads.into_iter().map(|payload| tree::HashDistance {
                    hash: payload.hash,
                    distance: payload.distance.clamp(0, max_distance),
                });

                let results = tree.find(hashes).await;

                client
                    .publish(reply, serde_json::to_vec(&results).unwrap_or_log().into())
                    .await
                    .unwrap_or_log();
            }
            .in_current_span(),
        );
    }

    Ok(())
}
