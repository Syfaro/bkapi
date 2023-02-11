use crate::{SearchResult, SearchResults};

/// The BKApi client, operating over NATS instead of HTTP.
#[derive(Clone)]
pub struct BKApiNatsClient {
    client: async_nats::Client,
}

/// A hash and distance.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct HashDistance {
    /// Hash to search.
    pub hash: i64,
    /// Maximum distance from hash to include in results.
    pub distance: u32,
}

impl BKApiNatsClient {
    const NATS_SUBJECT: &str = "bkapi.search";

    /// Create a new client with a given NATS client.
    pub fn new(client: async_nats::Client) -> Self {
        Self { client }
    }

    /// Search for a single hash.
    pub async fn search(
        &self,
        hash: i64,
        distance: i32,
    ) -> Result<SearchResults, async_nats::Error> {
        let hashes = [HashDistance {
            hash,
            distance: distance as u32,
        }];

        self.search_many(&hashes)
            .await
            .map(|mut results| results.remove(0))
    }

    /// Search many hashes at once.
    pub async fn search_many(
        &self,
        hashes: &[HashDistance],
    ) -> Result<Vec<SearchResults>, async_nats::Error> {
        let payload = serde_json::to_vec(hashes).unwrap();

        let message = self
            .client
            .request(Self::NATS_SUBJECT.to_string(), payload.into())
            .await?;

        let results: Vec<Vec<HashDistance>> = serde_json::from_slice(&message.payload).unwrap();

        let results = results
            .into_iter()
            .zip(hashes)
            .map(|(results, search)| SearchResults {
                hash: search.hash,
                distance: search.distance as u64,
                hashes: results
                    .into_iter()
                    .map(|result| SearchResult {
                        hash: result.hash,
                        distance: result.distance as u64,
                    })
                    .collect(),
            })
            .collect();

        Ok(results)
    }
}
