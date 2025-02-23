use clap::{Parser, Subcommand, ValueEnum};
use log;
use std::fmt::{Display, Formatter, Result};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Format {
    Parquet,
    Delta,
    Json,
    NDJson,
    Csv,
}

impl Display for Format {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match self {
            Format::Parquet => {
                write!(f, "parquet")
            }
            Format::Delta => {
                write!(f, "deltatable")
            }
            Format::Csv => {
                write!(f, "csv")
            }
            Format::Json => {
                write!(f, "json")
            }
            Format::NDJson => {
                write!(f, "ndjson")
            }
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum LogLevel {
    Off,
    Info,
    Debug,
}

/// cli parser
#[derive(Parser)]
#[command(name = "adt")]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[arg(short, long, value_enum, default_value_t = LogLevel::Info)]
    log_level: LogLevel,
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// view (and export) parquet or delta tables
    View {
        uri: String,
        #[arg(short, long, value_enum, default_value_t = Format::Delta)]
        format: Format,
        #[arg(short, long, default_value_t = String::from("select * from tbl"))]
        query: String,
        #[arg(short, long, default_value_t = 50)]
        limit: usize,
        #[arg(short, long)]
        partitions: Option<String>,
        // #[arg(short, long)]
        // output_path: Option<String>,
    },
    /// execute sql file
    Execute { sql_file: String },
    /// print parquet or delta table schema
    Schema {
        uri: String,
        #[arg(short, long, value_enum, default_value_t = Format::Delta)]
        format: Format,
        #[arg(short, long)]
        partitions: Option<String>,
    },
}

impl Cli {
    pub fn get_log_level(&self) -> Option<log::LevelFilter> {
        match self.log_level {
            LogLevel::Off => None,
            LogLevel::Info => Some(log::LevelFilter::Info),
            LogLevel::Debug => Some(log::LevelFilter::Debug),
        }
    }
}
