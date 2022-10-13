use std::sync::Arc;

use futures::TryStreamExt;
use sqlx::{postgres::PgListener, Pool, Postgres, Row};
use tokio::sync::RwLock;
use tracing_unwrap::ResultExt;

use crate::{Config, Error};

lazy_static::lazy_static! {
    static ref TREE_ADD_DURATION: prometheus::Histogram = prometheus::register_histogram!("bkapi_tree_add_duration_seconds", "Duration to add new item to tree").unwrap();
}

pub(crate) type Tree = Arc<RwLock<bk_tree::BKTree<Node, Hamming>>>;

/// A hamming distance metric.
pub(crate) struct Hamming;

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
pub(crate) struct Node([u8; 8]);

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

/// Create a new BK tree and pull in all hashes from provided query.
///
/// This must be called after you have started a listener, otherwise items may
/// be lost.
async fn create_tree(
    conn: &Pool<Postgres>,
    query: &str,
) -> Result<bk_tree::BKTree<Node, Hamming>, Error> {
    tracing::warn!("creating new tree");
    let mut tree = bk_tree::BKTree::new(Hamming);
    let mut rows = sqlx::query(query).fetch(conn);

    let mut count = 0;

    let start = std::time::Instant::now();

    while let Some(row) = rows.try_next().await.map_err(Error::LoadingRow)? {
        let node: Node = row.get::<i64, _>(0).into();

        if tree.find_exact(&node).is_none() {
            tree.add(node);
        }

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
pub(crate) async fn listen_for_payloads_db(
    conn: Pool<Postgres>,
    subscription: String,
    query: String,
    tree: Tree,
    initial: futures::channel::oneshot::Sender<()>,
) -> Result<(), Error> {
    let mut initial = Some(initial);

    loop {
        let mut listener = PgListener::connect_with(&conn)
            .await
            .map_err(Error::Listener)?;
        listener
            .listen(&subscription)
            .await
            .map_err(Error::Listener)?;

        let new_tree = create_tree(&conn, &query).await?;
        {
            let mut tree = tree.write().await;
            *tree = new_tree;
        }

        if let Some(initial) = initial.take() {
            initial
                .send(())
                .expect_or_log("nothing listening for initial data");
        }

        while let Some(notification) = listener.try_recv().await.map_err(Error::Listener)? {
            tracing::trace!("got postgres payload");
            process_payload(&tree, notification.payload().as_bytes()).await?;
        }

        tracing::error!("disconnected from postgres listener, recreating tree");
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}

pub(crate) async fn listen_for_payloads_nats(
    config: Config,
    pool: sqlx::PgPool,
    client: async_nats::Client,
    tree: Tree,
    initial: futures::channel::oneshot::Sender<()>,
) -> Result<(), Error> {
    static STREAM_NAME: &str = "bkapi-hashes";

    let jetstream = async_nats::jetstream::new(client);
    let mut initial = Some(initial);

    let stream = jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: STREAM_NAME.to_string(),
            subjects: vec!["bkapi.add".to_string()],
            max_age: std::time::Duration::from_secs(60 * 60 * 24),
            retention: async_nats::jetstream::stream::RetentionPolicy::Interest,
            ..Default::default()
        })
        .await?;

    loop {
        let consumer = stream
            .get_or_create_consumer(
                "bkapi-consumer",
                async_nats::jetstream::consumer::pull::Config {
                    ..Default::default()
                },
            )
            .await?;

        let new_tree = create_tree(&pool, &config.database_query).await?;
        {
            let mut tree = tree.write().await;
            *tree = new_tree;
        }

        if let Some(initial) = initial.take() {
            initial
                .send(())
                .expect_or_log("nothing listening for initial data");
        }

        let mut messages = consumer.messages().await?;

        while let Ok(Some(message)) = messages.try_next().await {
            tracing::trace!("got nats payload");
            message.ack().await?;
            process_payload(&tree, &message.payload).await?;
        }

        tracing::error!("disconnected from nats listener, recreating tree");
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}

async fn process_payload(tree: &Tree, payload: &[u8]) -> Result<(), Error> {
    let payload: Payload = serde_json::from_slice(payload).map_err(Error::Data)?;
    tracing::trace!("got hash: {}", payload.hash);

    let node: Node = payload.hash.into();

    let _timer = TREE_ADD_DURATION.start_timer();

    let is_new_hash = {
        let tree = tree.read().await;
        tree.find_exact(&node).is_none()
    };

    if is_new_hash {
        let mut tree = tree.write().await;
        tree.add(node);
    }

    tracing::debug!(hash = payload.hash, is_new_hash, "processed incoming hash");

    Ok(())
}
