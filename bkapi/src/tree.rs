use std::sync::Arc;

use sqlx::{postgres::PgListener, Pool, Postgres, Row};
use tokio::sync::RwLock;
use tracing_unwrap::ResultExt;

use crate::{Config, Error};

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
        let timer = crate::TREE_ADD_DURATION.start_timer();
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
pub(crate) async fn listen_for_payloads(
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

            let _timer = crate::TREE_ADD_DURATION.start_timer();

            let mut tree = tree.write().await;
            if tree.find_exact(&node).is_some() {
                tracing::trace!("hash already existed in tree");
                continue;
            }

            tracing::trace!("hash did not exist, adding to tree");
            tree.add(node);
        }

        tracing::error!("disconnected from listener, recreating tree");
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        let new_tree = create_tree(&conn, &config).await?;
        {
            let mut tree = tree.write().await;
            *tree = new_tree;
        }
    }
}
