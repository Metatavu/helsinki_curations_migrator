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
            continue; 
        }
        
        let document = if !curation.promoted.is_empty() {
            Some(elastic_client.get_document(curation.promoted.first().unwrap()).await.unwrap())
        } else {
            None
        };
        
        let mut put_request = PutRequest::builder()
            .item("id", AttributeValue::S(uuid::Uuid::new_v4().to_string()))
            .item("elasticCurationId", AttributeValue::S(curation.id.clone()))
            .item("curationType", AttributeValue::S(String::from("standard")))
            .item("promoted", AttributeValue::L(curation.promoted.clone().iter().map(|promoted| AttributeValue::S(promoted.clone())).collect()))
            .item("hidden", AttributeValue::L(curation.hidden.clone().iter().map(|hidden| AttributeValue::S(hidden.clone())).collect()))
            .item("queries", AttributeValue::L(curation.queries.clone().iter().map(|query| AttributeValue::S(query.clone())).collect()));
        let language;
        if let Some(doc) = document {
            put_request = put_request.item("name", AttributeValue::S(doc.title));
            if doc.language.is_some() {
                language = doc.language.unwrap()
            } else {
                language = String::from("fi")
            };       
        } else {
            language = String::from("fi");
        }
        
        put_request = put_request.item("language", AttributeValue::S(language));
        put_requests.push(put_request.build());
    }
    
    if put_requests.is_empty() {
        println!("No new curations to add!");
        return Ok(());
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
