use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResults {
    pub hash: i64,
    pub distance: u64,

    pub hashes: Vec<SearchResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResult {
    pub hash: i64,
    pub distance: u64,
}

pub struct BKApiClient {
    pub endpoint: String,
    client: reqwest::Client,
}

impl BKApiClient {
    pub fn new<E>(endpoint: E) -> Self
    where
        E: Into<String>,
    {
        Self {
            endpoint: endpoint.into(),
            client: reqwest::Client::default(),
        }
    }

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

    #[tracing::instrument(err, skip(self))]
    pub async fn search_many(
        &self,
        hashes: &[i64],
        distance: u64,
    ) -> Result<Vec<SearchResults>, reqwest::Error> {
        let mut futs = futures::stream::FuturesOrdered::new();
        for hash in hashes {
            futs.push(self.search(*hash, distance));
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
