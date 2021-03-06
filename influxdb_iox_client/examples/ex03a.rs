#[tokio::main]
async fn main() {
    use influxdb_iox_client::{connection::Builder, repl::Repl};

    let connection = Builder::default()
        .build("http://127.0.0.1:8082")
        .await
        .expect("client should be valid");

    // let mut client = Client::new(connection);

    let mut repl = Repl::new(connection);

    repl.use_database("bananas".to_string());
    let _output_format = repl.set_output_format("csv");

    let _x = repl
        .print_sql("select * from cpu".to_string())
        .await
        .expect("run_sql");

    //println!("{:?}", x);
}
