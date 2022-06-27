#[tokio::main]
async fn main() {
    use influxdb_iox_client::{
        connection::Builder,
        flight::{generated_types::ReadInfo, Client},
    };

    let connection = Builder::default()
        .build("http://127.0.0.1:8082")
        .await
        .expect("client should be valid");

    let mut client = Client::new(connection);

    let mut query_results = client
        .perform_query(ReadInfo {
            namespace_name: "postgresql:///iox_shared".to_string(),
            sql_query: "select * from h2o_temperature".to_string(),
        })
        .await
        .expect("query request should work");

    let mut batches = vec![];

    while let Some(data) = query_results.next().await.expect("valid batches") {
        batches.push(data);
    }

    println!("{:?}", batches);
}
