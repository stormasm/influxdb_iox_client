use std::{sync::Arc, time::Instant};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use observability_deps::tracing::{debug, info};
use snafu::{ResultExt, Snafu};

use crate::{connection::Connection, flight::generated_types::ReadInfo, format::QueryOutputFormat};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error loading remote state: {}", source))]
    LoadingRemoteState {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Error formatting results: {}", source))]
    FormattingResults { source: crate::format::Error },

    #[snafu(display("Error setting format to '{}': {}", requested_format, source))]
    SettingFormat {
        requested_format: String,
        source: crate::format::Error,
    },

    #[snafu(display("Error parsing command: {}", message))]
    ParsingCommand { message: String },

    #[snafu(display("Error running remote query: {}", source))]
    RunningRemoteQuery { source: crate::flight::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum QueryEngine {
    /// Run queries against the named database on the remote server
    Remote(String),
}

/// Captures the state of the repl, gathers commands and executes them
/// one by one
#[derive(Debug)]
pub struct Repl {
    /// Connection to the server
    #[allow(dead_code)]
    connection: Connection,

    /// Client for interacting with IOx namespace API
    #[allow(dead_code)]
    namespace_client: crate::namespace::Client,

    /// Client for running sql
    flight_client: crate::flight::Client,

    /// database name against which SQL commands are run
    query_engine: Option<QueryEngine>,

    /// Formatter to use to format query results
    output_format: QueryOutputFormat,
}

impl Repl {
    /// Create a new Repl instance, connected to the specified URL
    pub fn new(connection: Connection) -> Self {
        let namespace_client = crate::namespace::Client::new(connection.clone());
        let flight_client = crate::flight::Client::new(connection.clone());

        let output_format = QueryOutputFormat::Pretty;

        Self {
            connection,
            namespace_client,
            flight_client,
            query_engine: None,
            output_format,
        }
    }

    // print all namespaces to the output
    #[allow(dead_code)]
    async fn list_namespaces(&mut self) -> Result<()> {
        let namespaces = self
            .namespace_client
            .get_namespaces()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(LoadingRemoteStateSnafu)?;

        let namespace_id: Int64Array = namespaces.iter().map(|ns| Some(ns.id)).collect();
        let name: StringArray = namespaces.iter().map(|ns| Some(&ns.name)).collect();

        let record_batch = RecordBatch::try_from_iter(vec![
            ("namespace_id", Arc::new(namespace_id) as ArrayRef),
            ("name", Arc::new(name) as ArrayRef),
        ])
        .expect("creating record batch successfully");

        self.print_results(&[record_batch])
    }

    // Run a command against the currently selected remote database
    pub async fn run_sql(&mut self, sql: String) -> Result<String> {
        let batches = match &mut self.query_engine {
            None => {
                println!("Error: no database selected.");
                println!("Hint: Run USE DATABASE <dbname> to select database");
                return Ok("Error: no database selected".to_string());
            }
            Some(QueryEngine::Remote(db_name)) => {
                info!(%db_name, %sql, "Running sql on remote database");

                scrape_query(&mut self.flight_client, db_name, &sql).await?
            }
        };

        let result_str = self.get_results(&batches)?;

        Ok(result_str)
    }

    // Run a command against the currently selected remote database
    pub async fn print_sql(&mut self, sql: String) -> Result<()> {
        let start = Instant::now();

        let batches = match &mut self.query_engine {
            None => {
                println!("Error: no database selected.");
                println!("Hint: Run USE DATABASE <dbname> to select database");
                return Ok(());
            }
            Some(QueryEngine::Remote(db_name)) => {
                info!(%db_name, %sql, "Running sql on remote database");

                scrape_query(&mut self.flight_client, db_name, &sql).await?
            }
        };

        let end = Instant::now();
        self.print_results(&batches)?;

        println!(
            "Returned {} in {:?}",
            Self::row_summary(&batches),
            end - start
        );
        Ok(())
    }

    fn row_summary<'a>(batches: impl IntoIterator<Item = &'a RecordBatch>) -> String {
        let total_rows: usize = batches.into_iter().map(|b| b.num_rows()).sum();

        if total_rows > 1 {
            format!("{} rows", total_rows)
        } else if total_rows == 0 {
            "no rows".to_string()
        } else {
            "1 row".to_string()
        }
    }

    pub fn use_database(&mut self, db_name: String) {
        debug!(%db_name, "setting current database");
        println!("You are now in remote mode, querying database {}", db_name);
        self.set_query_engine(QueryEngine::Remote(db_name));
    }

    pub fn set_query_engine(&mut self, query_engine: QueryEngine) {
        self.query_engine = Some(query_engine)
    }

    /// Sets the output format to the specified format
    pub fn set_output_format<S: AsRef<str>>(&mut self, requested_format: S) -> Result<()> {
        let requested_format = requested_format.as_ref();

        self.output_format = requested_format
            .parse()
            .context(SettingFormatSnafu { requested_format })?;
        println!("Set output format format to {}", self.output_format);
        Ok(())
    }

    /// Prints to the specified output format
    fn get_results(&self, batches: &[RecordBatch]) -> Result<String> {
        let formatted_results = self
            .output_format
            .format(batches)
            .context(FormattingResultsSnafu)?;
        println!("{}", formatted_results);
        Ok(formatted_results)
    }

    /// Prints to the specified output format
    fn print_results(&self, batches: &[RecordBatch]) -> Result<()> {
        let formatted_results = self
            .output_format
            .format(batches)
            .context(FormattingResultsSnafu)?;
        println!("{}", formatted_results);
        Ok(())
    }
}

/// Runs the specified `query` and returns the record batches of the result
async fn scrape_query(
    client: &mut crate::flight::Client,
    db_name: &str,
    query: &str,
) -> Result<Vec<RecordBatch>> {
    let mut query_results = client
        .perform_query(ReadInfo {
            namespace_name: db_name.to_string(),
            sql_query: query.to_string(),
        })
        .await
        .context(RunningRemoteQuerySnafu)?;

    let mut batches = vec![];

    while let Some(data) = query_results
        .next()
        .await
        .context(RunningRemoteQuerySnafu)?
    {
        batches.push(data);
    }

    Ok(batches)
}
