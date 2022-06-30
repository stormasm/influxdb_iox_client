use tokio::runtime::Runtime;

fn main() -> Result<(), std::io::Error> {
    let _x = tokio_block();
    Ok(())
}

fn tokio_block() -> Result<(), std::io::Error> {
    use influxdb_iox_client::{connection::Builder, health::Client};

    let num_threads: Option<usize> = None;

    let tokio_runtime = get_runtime(num_threads)?;
    tokio_runtime.block_on(async move {
        let connection = Builder::default()
            .build("http://127.0.0.1:8082")
            .await
            .unwrap();

        let mut client = Client::new(connection);

        let x = client.check_storage().await.expect("check_storage failure");
        println!("{:?}", x);
    });

    Ok(())
}

/// Creates the tokio runtime for executing IOx
///
/// if nthreads is none, uses the default scheduler
/// otherwise, creates a scheduler with the number of threads
fn get_runtime(num_threads: Option<usize>) -> Result<Runtime, std::io::Error> {
    // NOTE: no log macros will work here!
    //
    // That means use eprintln!() instead of error!() and so on. The log emitter
    // requires a running tokio runtime and is initialised after this function.

    use tokio::runtime::Builder;
    let kind = std::io::ErrorKind::Other;
    match num_threads {
        None => Runtime::new(),
        Some(num_threads) => {
            println!(
                "Setting number of threads to '{}' per command line request",
                num_threads
            );

            match num_threads {
                0 => {
                    let msg = format!(
                        "Invalid num-threads: '{}' must be greater than zero",
                        num_threads
                    );
                    Err(std::io::Error::new(kind, msg))
                }
                1 => Builder::new_current_thread().enable_all().build(),
                _ => Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(num_threads)
                    .build(),
            }
        }
    }
}
