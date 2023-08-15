use std::sync::Arc;

use async_nats::jetstream::consumer::DeliverPolicy;
use bk_tree::BKTree;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgListener, Pool, Postgres, Row};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing_unwrap::ResultExt;

use crate::{Config, Error, NatsError};

lazy_static::lazy_static! {
    static ref TREE_ENTRIES: prometheus::IntCounter = prometheus::register_int_counter!("bkapi_tree_entries", "Total number of entries within tree").unwrap();

    static ref TREE_ADD_DURATION: prometheus::Histogram = prometheus::register_histogram!("bkapi_tree_add_duration_seconds", "Duration to add new item to tree").unwrap();
    static ref TREE_DURATION: prometheus::HistogramVec = prometheus::register_histogram_vec!("bkapi_tree_duration_seconds", "Duration of tree search time", &["distance"]).unwrap();
}

/// A BKTree wrapper to cover common operations.
#[derive(Clone)]
pub struct Tree {
    pub tree: Arc<RwLock<BKTree<Node, Hamming>>>,
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

        TREE_ENTRIES.reset();
        TREE_ENTRIES.inc_by(count);

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
            TREE_ENTRIES.inc();
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
#[derive(Serialize, Deserialize)]
pub struct Hamming;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node([u8; 8]);

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
#[tracing::instrument(skip_all)]
pub(crate) async fn listen_for_payloads_db(
    conn: Pool<Postgres>,
    subscription: String,
    query: String,
    tree: Tree,
    initial: futures::channel::oneshot::Sender<()>,
    token: CancellationToken,
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

        loop {
            tokio::select! {
                res = listener.try_recv() => {
                    match res {
                        Ok(Some(notification)) => {
                            tracing::trace!("got postgres payload");
                            process_payload(&tree, notification.payload().as_bytes()).await?;
                        }
                        Ok(None) => {
                            tracing::warn!("got none value from recv");
                            break;
                        }
                        Err(err) => {
                            tracing::error!("got recv error: {err}");
                            break;
                        }
                    }
                }
                _ = token.cancelled() => {
                    tracing::info!("got cancellation");
                    break;
                }
            }
        }

        if token.is_cancelled() {
            tracing::info!("cancelled, stopping db listener");
            return Ok(());
        } else {
            tracing::error!("disconnected from postgres listener, recreating tree");
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
    }
}

/// Listen for incoming payloads from NATS.
#[tracing::instrument(skip_all)]
pub(crate) async fn listen_for_payloads_nats(
    config: Config,
    pool: sqlx::PgPool,
    client: async_nats::Client,
    tree: Tree,
    initial: futures::channel::oneshot::Sender<()>,
    token: CancellationToken,
) -> Result<(), Error> {
    let jetstream = async_nats::jetstream::new(client);
    let mut initial = Some(initial);

    let stream = jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: format!(
                "{}-bkapi-hashes",
                config.nats_prefix.clone().unwrap().replace('.', "-")
            ),
            subjects: vec![format!("{}.bkapi.add", config.nats_prefix.unwrap())],
            max_age: std::time::Duration::from_secs(60 * 30),
            retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
            ..Default::default()
        })
        .await
        .map_err(NatsError::CreateStream)?;

    // Because we're tracking the last sequence ID before we load tree data,
    // we don't need to start the listener until after it's loaded. This
    // prevents issues with a slow client but still retains every hash.
    let mut seq = stream.cached_info().state.last_sequence;

    let create_consumer = |stream: async_nats::jetstream::stream::Stream, start_sequence: u64| async move {
        tracing::info!(start_sequence, "creating consumer");

        stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                deliver_policy: if start_sequence > 0 {
                    DeliverPolicy::ByStartSequence { start_sequence }
                } else {
                    DeliverPolicy::All
                },
                ..Default::default()
            })
            .await
            .map_err(NatsError::Consumer)
    };

    tree.reload(&pool, &config.database_query).await?;

    if let Some(initial) = initial.take() {
        initial
            .send(())
            .expect_or_log("nothing listening for initial data");
    }

    loop {
        let consumer = create_consumer(stream.clone(), seq).await?;

        let messages = consumer
            .messages()
            .await
            .map_err(NatsError::Stream)?
            .take_until(token.cancelled());
        tokio::pin!(messages);

        while let Ok(Some(message)) = messages.try_next().await {
            process_payload(&tree, &message.payload).await?;

            message.ack().await.map_err(NatsError::Generic)?;
            seq = message
                .info()
                .expect_or_log("message missing info")
                .stream_sequence;
        }

        if token.is_cancelled() {
            tracing::info!("cancelled, stopping nats listener");
            return Ok(());
        } else {
            tracing::error!("disconnected from nats listener");
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
    }
}

/// Process a payload from Postgres or NATS and add to the tree.
#[tracing::instrument(skip_all)]
async fn process_payload(tree: &Tree, payload: &[u8]) -> Result<(), Error> {
    tracing::trace!("got payload: {}", String::from_utf8_lossy(payload));

    let payload: Payload = serde_json::from_slice(payload).map_err(Error::Data)?;
    tracing::trace!("got hash: {}", payload.hash);

    let _timer = TREE_ADD_DURATION.start_timer();
    tree.add(payload.hash).await;

    Ok(())
}
