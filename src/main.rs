use std::sync::Arc;
use std::time::Instant;

use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use datafusion::dataframe::DataFrameWriteOptions;
use log::{error, info};
use simple_logger::SimpleLogger;

mod cli;
mod table;
mod tui;
mod utils;

use crate::cli::{Cli, Commands};
use crate::table::TableContext;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let logger = SimpleLogger::new();

    match cli.get_log_level() {
        Some(level) => logger.with_level(level).init().unwrap(),
        None => {}
    }

    match &cli.command {
        Commands::View {
            table_path,
            format,
            query,
            partitions,
            limit,
            no_tui,
            output_path,
        } => {
            let tblctx = Arc::new(TableContext::new(
                table_path.as_str(),
                partitions,
                format.clone(),
            ));
            let req_time = Instant::now();
            tblctx
                .register_table()
                .await
                .expect("Table registration fails");
            let req_time_elapsed = req_time.elapsed();
            info!("Table registration time: {:.2?}", req_time_elapsed);
            let req_time = Instant::now();
            let df = tblctx
                .exec_query(query.clone(), limit.clone())
                .await
                .expect("Query execution fails");
            let records = df
                .clone()
                .collect()
                .await
                .expect("Unable to collect dataframe records");
            let req_time_elapsed = req_time.elapsed();
            info!("Query execution time: {:.2?}", req_time_elapsed);
            if *no_tui {
                println!(
                    "{}",
                    pretty_format_batches(&records).expect("Pretty format fails")
                );
            } else {
                let _ = tui::show_in_tui(
                    pretty_format_batches(&records)
                        .unwrap()
                        .to_string()
                        .as_str(),
                );
            }
            if let Some(op) = output_path {
                let ext = std::path::Path::new(op)
                    .extension()
                    .expect("Unable to extract file extension")
                    .to_str();
                match ext {
                    Some("csv") => {
                        info!("export to csv");
                        let _ = df
                            .write_csv(
                                op,
                                DataFrameWriteOptions::default().with_single_file_output(true),
                                None,
                            )
                            .await
                            .unwrap();
                        {}
                    }
                    Some("json") => {
                        info!("export to newline delimited json");
                        let _ = df
                            .write_json(
                                op,
                                DataFrameWriteOptions::default().with_single_file_output(true),
                                None,
                            )
                            .await
                            .unwrap();
                        {}
                    }
                    _ => error!("Unsupported output format"),
                }
            }
        }
        Commands::Schema {
            table_path,
            partitions,
            format,
            no_tui,
        } => {
            let tblctx = Arc::new(TableContext::new(
                table_path.as_str(),
                partitions,
                format.clone(),
            ));
            let req_time = Instant::now();
            tblctx
                .register_table()
                .await
                .expect("Table registration fails");
            let req_time_elapsed = req_time.elapsed();
            info!("Table registration time: {:.2?}", req_time_elapsed);
            let req_time = Instant::now();
            let records = tblctx
                .schema()
                .await
                .expect("Schema query fails")
                .collect()
                .await
                .expect("Schema collect fails");
            let req_time_elapsed = req_time.elapsed();
            info!("Query execution time: {:.2?}", req_time_elapsed);
            if *no_tui {
                println!(
                    "{}",
                    pretty_format_batches(&records).expect("Pretty format fails")
                );
            } else {
                let _ = tui::show_in_tui(
                    pretty_format_batches(&records)
                        .unwrap()
                        .to_string()
                        .as_str(),
                );
            }
        }
        Commands::Explain {
            table_path,
            format,
            query,
            limit,
            partitions,
        } => {
            // Create table context
            let tblctx = Arc::new(TableContext::new(
                table_path.as_str(),
                partitions,
                format.clone(),
            ));
            tblctx
                .register_table()
                .await
                .expect("Table registration fails");
            // parse the SQL
            let full_query = tblctx.build_query(query.clone(), limit.clone());
            let initial_plan = tblctx
                .context()
                .state()
                .create_logical_plan(full_query.as_ref())
                .await
                .unwrap();
            // show the plan
            println!("Initial Plan:\n{:?}", initial_plan.clone());

            let optimized_plan = tblctx.context().state().optimize(&initial_plan);

            // show the plan
            println!("Optimized Plan:\n{:?}", optimized_plan.unwrap());
        }
    }
}
