use aws_sdk_dynamodb::{model::{AttributeValue, PutRequest, WriteRequest}};
use clap::Parser;
use elastic::ElasticCurations;
mod elastic;

#[derive(Parser, Debug)]
#[command(author = "Ville Juutila", version = "1.0.0", about = "Gets Curations from Elastic App Search and persists them into AWS DynamoDB.")]
struct Args {
    #[arg(short, long)]
    url: String,
    #[arg(short, long)]
    api_key: String,
    #[arg(short, long)]
    engine: String,
    #[arg(short, long)]
    region: String
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let aws_config = aws_config::from_env().region(aws_types::region::Region::new(args.region)).load().await;
    let dynamodb_client = aws_sdk_dynamodb::Client::new(&aws_config);
    let elastic_client = elastic::AppSearchClient::new(
        args.url.clone(),
        args.engine.clone(),
        args.api_key.clone()
    );
    
    let mut curations: Vec<ElasticCurations> = Vec::new();
    let mut current_page = 1;
    println!("Proceeding to get Curations from Elastic...");
    
    loop {
        let curations_response = elastic_client.get_curations(&current_page).await;
        curations.append(&mut curations_response.results.clone());
        println!("Retrieved page {}/{} of Elastic Curations!", &curations_response.meta.page.current, &curations_response.meta.page.total_pages);
        if &curations_response.meta.page.current == &curations_response.meta.page.total_pages {
            println!("All {} Elastic Curations retrieved!", &curations.len());
            break;
        }
        current_page = &current_page + 1;
    }
    
    println!("Proceeding to persist Curations in AWS...");
    let mut failed_curations = 0;
    let mut put_requests: Vec<PutRequest> = Vec::new();
    println!("Total Elastic Curations: {}", &curations.len());
    for curation in &curations {
        let scan_res = dynamodb_client
            .scan()
            .table_name("curations")
            .filter_expression("elasticCurationId = :eci")
            .expression_attribute_values(
                ":eci",
                AttributeValue::S(curation.id.clone())
            )
            .send()
            .await?;
        if scan_res.count() > 0 {
            break;   
        }
        
        let empty_string = String::new();
        let document_id = curation.promoted.first().unwrap_or_else(|| &empty_string);
        let id_av = AttributeValue::S(uuid::Uuid::new_v4().to_string());
        let promoted_av = AttributeValue::Ss(curation.promoted.clone());
        let hidden_av = AttributeValue::Ss(curation.hidden.clone());
        let queries_av = AttributeValue::Ss(curation.queries.clone());
        let document_id_av = AttributeValue::S(document_id.clone());
        let elastic_curation_id_av = AttributeValue::S(document_id.clone());
        let curation_type_av = AttributeValue::S(String::from("standard"));
        
        let mut put_request = PutRequest::builder()
            .item("id", id_av)
            .item("elasticCurationId", elastic_curation_id_av)
            .item("curationType", curation_type_av);
        if !curation.promoted.is_empty() {
            put_request = put_request.item("promoted", promoted_av);
        } 
        if !curation.hidden.is_empty() {
            put_request = put_request.item("hidden", hidden_av);
        }
        if !curation.queries.is_empty() {
            put_request = put_request.item("queries", queries_av);
        }
        if !document_id.is_empty() {
            put_request = put_request.item("documentId", document_id_av);
        }
        
        put_requests.push(put_request.build());
    }
    
    println!("Splitting requests into chunks...");
    for (pos, put_request_chunk) in put_requests.chunks(20).enumerate() {
        println!("Persisting chunk {}...", pos + 1);
        let current_items: Vec<WriteRequest> = put_request_chunk.iter().map(|request| {
            WriteRequest::builder()
                .put_request(request.clone()).build()
        }).collect();
        let put_response = dynamodb_client
            .batch_write_item()
            .request_items("curations", current_items)
            .send()
            .await;

        match put_response {
            Ok(resp) => failed_curations += resp.unprocessed_items().unwrap().len(),
            Err(error) => {
                println!("{:#?}", error.into_service_error());
            }
        }
    }

    println!("Failed to persist {} Curations!", &failed_curations);
    println!("Done!");
    Ok(())
}
