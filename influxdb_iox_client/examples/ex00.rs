use influxdb_iox_client::{connection::Connection, health};

use snafu::{ResultExt, Snafu};

#[tokio::main]
async fn main() {
    let builder = influxdb_iox_client::connection::Builder::default();
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Health check request failed: {}", source))]
    Client {
        source: influxdb_iox_client::error::Error,
    },

    #[snafu(display("Storage service not running"))]
    StorageNotRunning,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

async fn check_health(connection: Connection) -> Result<()> {
    let response = health::Client::new(connection)
        .check_arrow()
        .await
        .context(ClientSnafu)?;

    match response {
        true => Ok(()),
        false => Err(Error::StorageNotRunning),
    }
}
