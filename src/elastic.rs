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
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ElasticDocument {
  pub id: String,
  pub title: String,
  pub language: Option<String>
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
  
  pub async fn get_document(&self, document_id: &str) -> Option<ElasticDocument> {
    let response = self.client
      .get(format!("{}/documents?ids[]={document_id}", self.url))
      .bearer_auth(&self.api_key)
      .send()
      .await
      .unwrap()
      .json::<Vec<ElasticDocument>>()
      .await;
    match response {
      Result::Ok(docs) => if !docs.is_empty() {
        Some(docs.first().unwrap().to_owned())
      } else {
        println!("Couldn't find Elastic Document with id {document_id}");
        None
      },
      Result::Err(error) => {
        println!("Error while getting Elastic Document with id {document_id}: {error}");
        None
      }
    }
  }
}