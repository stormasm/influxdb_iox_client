#[tokio::main]
async fn main() {
    use influxdb_iox_client::{
        connection::Builder,
        flight::{generated_types::ReadInfo, Client},
        repl::Repl,
    };

    let connection = Builder::default()
        .build("http://127.0.0.1:8082")
        .await
        .expect("client should be valid");

    // let mut client = Client::new(connection);

    let mut repl = Repl::new(connection);

    repl.use_database("postgresql:///iox_shared".to_string());

    let x = repl
        .run_sql("select * from h2o_temperature".to_string())
        .await
        .expect("run_sql");

    println!("{:?}", x);
}
