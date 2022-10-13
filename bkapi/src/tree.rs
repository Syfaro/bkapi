use std::sync::Arc;

use bk_tree::BKTree;
use futures::TryStreamExt;
use sqlx::{postgres::PgListener, Pool, Postgres, Row};
use tokio::sync::RwLock;
use tracing_unwrap::ResultExt;

use crate::{Config, Error};

lazy_static::lazy_static! {
    static ref TREE_ADD_DURATION: prometheus::Histogram = prometheus::register_histogram!("bkapi_tree_add_duration_seconds", "Duration to add new item to tree").unwrap();
    static ref TREE_DURATION: prometheus::HistogramVec = prometheus::register_histogram_vec!("bkapi_tree_duration_seconds", "Duration of tree search time", &["distance"]).unwrap();
}

/// A BKTree wrapper to cover common operations.
#[derive(Clone)]
pub struct Tree {
    tree: Arc<RwLock<BKTree<Node, Hamming>>>,
}

/// A hash and distance pair. May be used for searching or in search results.
#[derive(serde::Serialize)]
pub struct HashDistance {
    pub hash: i64,
    pub distance: u32,
}

impl Tree {
    /// Create an empty tree.
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BKTree::new(Hamming))),
        }
    }

    /// Replace tree contents with the results of a SQL query.
    ///
    /// The tree is only replaced after it finishes loading, leaving stale/empty
    /// data available while running.
    pub(crate) async fn reload(&self, pool: &sqlx::PgPool, query: &str) -> Result<(), Error> {
        let mut new_tree = BKTree::new(Hamming);
        let mut rows = sqlx::query(query).fetch(pool);

        let start = std::time::Instant::now();
        let mut count = 0;

        while let Some(row) = rows.try_next().await.map_err(Error::LoadingRow)? {
            let node: Node = row.get::<i64, _>(0).into();

            if new_tree.find_exact(&node).is_none() {
                new_tree.add(node);
            }

            count += 1;
            if count % 250_000 == 0 {
                tracing::debug!(count, "loaded more rows");
            }
        }

        let dur = std::time::Instant::now().duration_since(start);
        tracing::info!(count, "completed loading rows in {:?}", dur);

        let mut tree = self.tree.write().await;
        *tree = new_tree;

        Ok(())
    }

    /// Add a hash to the tree, returning if it already existed.
    #[tracing::instrument(skip(self))]
    pub async fn add(&self, hash: i64) -> bool {
        let node = Node::from(hash);

        let is_new_hash = {
            let tree = self.tree.read().await;
            tree.find_exact(&node).is_none()
        };

        if is_new_hash {
            let mut tree = self.tree.write().await;
            tree.add(node);
        }

        tracing::info!(is_new_hash, "added hash");

        is_new_hash
    }

    /// Attempt to find any number of hashes within the tree.
    pub async fn find<H>(&self, hashes: H) -> Vec<Vec<HashDistance>>
    where
        H: IntoIterator<Item = HashDistance>,
    {
        let tree = self.tree.read().await;

        hashes
            .into_iter()
            .map(|HashDistance { hash, distance }| Self::search(&tree, hash, distance))
            .collect()
    }

    /// Search a read-locked tree for a hash with a given distance.
    #[tracing::instrument(skip(tree))]
    fn search(tree: &BKTree<Node, Hamming>, hash: i64, distance: u32) -> Vec<HashDistance> {
        tracing::debug!("searching tree");

        let duration = TREE_DURATION
            .with_label_values(&[&distance.to_string()])
            .start_timer();
        let results: Vec<_> = tree
            .find(&hash.into(), distance)
            .into_iter()
            .map(|item| HashDistance {
                distance: item.0,
                hash: (*item.1).into(),
            })
            .collect();
        let time = duration.stop_and_record();

        tracing::info!(time, results = results.len(), "found results");
        results
    }
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

/// A payload for a new hash to add to the index from Postgres or NATS.
#[derive(serde::Deserialize)]
struct Payload {
    hash: i64,
}

/// Listen for incoming payloads from Postgres.
#[tracing::instrument(skip(conn, subscription, query, tree, initial))]
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

        tree.reload(&conn, &query).await?;

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

/// Listen for incoming payloads from NATS.
#[tracing::instrument(skip(config, pool, client, tree, initial))]
pub(crate) async fn listen_for_payloads_nats(
    config: Config,
    pool: sqlx::PgPool,
    client: async_nats::Client,
    tree: Tree,
    initial: futures::channel::oneshot::Sender<()>,
) -> Result<(), Error> {
    let jetstream = async_nats::jetstream::new(client);
    let mut initial = Some(initial);

    let stream = jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: "bkapi-hashes".to_string(),
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

        tree.reload(&pool, &config.database_query).await?;

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

/// Process a payload from Postgres or NATS and add to the tree.
async fn process_payload(tree: &Tree, payload: &[u8]) -> Result<(), Error> {
    let payload: Payload = serde_json::from_slice(payload).map_err(Error::Data)?;
    tracing::trace!("got hash: {}", payload.hash);

    let _timer = TREE_ADD_DURATION.start_timer();
    tree.add(payload.hash).await;

    Ok(())
}
