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
        .write_lp("bananas", "cpu,region=west user=23.2 100", 0)
        .await
        .expect("failed to write to IOx");
}
