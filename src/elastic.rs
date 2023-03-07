use serde::{Serialize, Deserialize};

const BASE_URL_SUFFIX: &str = "/api/as/v1/engines/";

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ElasticCurationsResponse {
  pub meta: ElasticMeta,
  pub results: Vec<ElasticCurations>
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ElasticMeta {
  pub page: ElasticMetaPage,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ElasticMetaPage {
  pub current: i32,
  pub total_pages: i32,
  pub total_results: i32,
  pub size: i32
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ElasticCurations {
  pub id: String,
  pub queries: Vec<String>,
  pub promoted: Vec<String>,
  pub hidden: Vec<String>,
  #[serde(default)]
  pub suggestion: Option<ElasticCurationsSuggestion>
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ElasticCurationsSuggestion {
  pub created_at: String,
  pub operation: String,
  pub promoted: Vec<String>,
  pub status: String,
  pub updated_at: String
}

#[derive(Clone, Debug)]
pub struct AppSearchClient {
  pub url: String,
  api_key: String,
  client: reqwest::Client
}

impl AppSearchClient {
  
  pub fn new(
    base_url: String,
    engine: String,
    api_key: String
  ) -> AppSearchClient {
    AppSearchClient {
      url: format!("{base}/{suffix}/{engine}", 
        base = base_url,
        suffix = BASE_URL_SUFFIX,
        engine = engine
      ),
      api_key: api_key,
      client: reqwest::Client::new()
    }
  }
  
  pub async fn get_curations(&self, page_number: &i32) -> ElasticCurationsResponse {
    self.client
      .get(format!("{}/curations?page[current]={}", self.url, page_number))
      .bearer_auth(&self.api_key)
      .send()
      .await
      .unwrap()
      .json::<ElasticCurationsResponse>()
      .await
      .unwrap()
  }
}