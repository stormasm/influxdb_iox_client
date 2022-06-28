#[tokio::main]
async fn main() {
    use influxdb_iox_client::{connection::Builder, write::Client};

    let connection = Builder::default()
        .build("http://127.0.0.1:8081")
        .await
        .unwrap();

    let mut client = Client::new(connection);

    // write a line of line procol data
    client
        .write_lp("bananas", "cpu,region=south user=50.32 20000000", 0)
        .await
        .expect("failed to write to IOx");
}
