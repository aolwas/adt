use std::fs;
use std::io::{BufRead, BufReader};

use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use log::debug;
use minijinja::render;
use simple_logger::SimpleLogger;
use snafu::Whatever;

use adt_core::context::SQLContext;

mod cli;
use crate::cli::{Cli, Commands};

async fn execute(ctx: &SQLContext, sql: &str, with_output: bool) -> Result<(), Whatever> {
    let df = ctx.sql(sql).await.expect("Query execution fails");
    let records = df
        .clone()
        .collect()
        .await
        .expect("Unable to collect dataframe records");
    if with_output {
        println!(
            "{}",
            pretty_format_batches(&records).expect("Pretty format fails")
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let logger = SimpleLogger::new();

    match cli.get_log_level() {
        Some(level) => logger.with_level(level).init().unwrap(),
        _ => {}
    }

    let ctx = SQLContext::new();

    match &cli.command {
        Commands::View {
            uri,
            format,
            query,
            partitions,
            limit,
            //output_path,
        } => {
            let ddl = render!(
            r#"
            create external table tbl
            stored as {{ fmt }}
            {{ part_spec }}
            location '{{ uri }}'
            "#,
            fmt => format.to_string(),
            part_spec => match partitions {
                Some(p) => format!("partitioned by ({})",p),
                None => "".into()
            } ,
            uri => uri
            );
            debug!("ddl statement: {}", ddl);
            execute(&ctx, ddl.as_str(), false)
                .await
                .expect("ddl statement fails");
            execute(&ctx, format!("{} limit {}", query, limit).as_str(), true)
                .await
                .expect("query statement fails");
        }
        Commands::Schema {
            uri,
            format,
            partitions,
        } => {
            let ddl = render!(
            r#"
            create external table tbl
            stored as {{ fmt }}
            {{ part_spec }}
            location '{{ uri }}'
            "#,
            fmt => format.to_string(),
            part_spec => match partitions {
                Some(p) => format!("partitioned by ({})",p),
                None => "".into()
            } ,
            uri => uri
            );
            debug!("ddl statement: {}", ddl);
            execute(&ctx, ddl.as_str(), false)
                .await
                .expect("ddl statement fails");
            execute(&ctx, "show columns from tbl".into(), true)
                .await
                .expect("query statement fails");
        }
        Commands::Execute { sql_file } => {
            let mut query = "".to_owned();
            let file = fs::File::open(sql_file);
            let reader = BufReader::new(file.unwrap());
            for line in reader.lines() {
                match line {
                    Ok(line) if line.starts_with("--") => {
                        continue;
                    }
                    Ok(line) => {
                        let line = line.trim_end();
                        query.push_str(line);
                        if line.ends_with(';') {
                            execute(&ctx, &query, true)
                                .await
                                .expect("Query execution fails");
                            query = "".to_string();
                        } else {
                            query.push('\n');
                        }
                    }
                    _ => {
                        break;
                    }
                }
            }

            // run the left over query if the last statement doesn't contain ‘;’
            // ignore if it only consists of '\n'
            if query.contains(|c| c != '\n') {
                execute(&ctx, &query, true)
                    .await
                    .expect("Query execution fails");
            }
        }
    }
}
