#![deny(missing_docs)]

//! A client for BKApi.
//!
//! Provides basic types and a HTTP client for searching a BKApi instance.

use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

/// A search result, containing the searched information and all of the results.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResults {
    /// Searched hash.
    pub hash: i64,
    /// Searched distance.
    pub distance: u64,

    /// Search results.
    pub hashes: Vec<SearchResult>,
}

/// A single search result, containing information about the match.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResult {
    /// Result hash.
    pub hash: i64,
    /// Distance between search and this result.
    pub distance: u64,
}

/// The BKApi client.
#[derive(Clone)]
pub struct BKApiClient {
    /// Endpoint to search for results.
    pub endpoint: String,
    client: reqwest::Client,
}

impl BKApiClient {
    /// Create a new BKApi client.
    ///
    /// Endpoint should be the full path to the `/search` endpoint.
    ///
    /// ```rust
    /// let bkapi = BKApiClient::new("http://bkapi:3000/search");
    /// ```
    pub fn new<E>(endpoint: E) -> Self
    where
        E: Into<String>,
    {
        Self {
            endpoint: endpoint.into(),
            client: reqwest::Client::default(),
        }
    }

    /// Search for a hash with a given maximum distance.
    #[tracing::instrument(err, skip(self))]
    pub async fn search(&self, hash: i64, distance: u64) -> Result<SearchResults, reqwest::Error> {
        let results = self
            .client
            .get(&self.endpoint)
            .query(&[
                ("hash", hash.to_string()),
                ("distance", distance.to_string()),
            ])
            .inject_context()
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(results)
    }

    /// Search for multiple hashes given a single maximum distance.
    ///
    /// Results are returned in the same order as given hashes.
    #[tracing::instrument(err, skip(self))]
    pub async fn search_many(
        &self,
        hashes: &[i64],
        distance: u64,
    ) -> Result<Vec<SearchResults>, reqwest::Error> {
        let mut futs = futures::stream::FuturesOrdered::new();
        for hash in hashes {
            futs.push_back(self.search(*hash, distance));
        }

        futs.try_collect().await
    }
}

trait InjectContext {
    fn inject_context(self) -> Self;
}

impl InjectContext for reqwest::RequestBuilder {
    fn inject_context(self: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        let mut headers: reqwest::header::HeaderMap = Default::default();

        let cx = tracing::Span::current().context();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&cx, &mut opentelemetry_http::HeaderInjector(&mut headers))
        });

        self.headers(headers)
    }
}

#[cfg(test)]
mod tests {
    fn get_test_endpoint() -> String {
        std::env::var("BKAPI_ENDPOINT").expect("tests require BKAPI_ENDPOINT to be set")
    }

    #[tokio::test]
    async fn test_search() {
        let endpoint = get_test_endpoint();
        let client = super::BKApiClient::new(endpoint);

        let results = client.search(0, 3).await;
        println!("{:?}", results);
        let results = results.expect("search should not have error");
        assert_eq!(
            0, results.hash,
            "returned hash should have been {} but was {}",
            0, results.hash
        );
        assert_eq!(
            3, results.distance,
            "returned distance should have been {} but was {}",
            3, results.distance
        );
    }

    #[tokio::test]
    async fn test_search_many() {
        let endpoint = get_test_endpoint();
        let client = super::BKApiClient::new(endpoint);

        let results = client.search_many(&[0, 1_000_000], 7).await;
        println!("{:?}", results);
        let results = results.expect("search should not have error");
        assert_eq!(
            2,
            results.len(),
            "returned results were missing search, expected {} items but found {} items",
            2,
            results.len()
        );
    }
}
