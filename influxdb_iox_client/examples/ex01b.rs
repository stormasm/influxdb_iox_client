#[tokio::main]
async fn main() {
    use influxdb_iox_client::{connection::Builder, health::Client};

    let connection = Builder::default()
        .build("http://127.0.0.1:8082")
        .await
        .unwrap();

    let mut client = Client::new(connection);

    let x = client.check_storage().await.expect("check_storage failure");
    println!("{:?}", x);
}
