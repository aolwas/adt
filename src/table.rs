use anyhow::Result;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionConfig;
use datafusion::prelude::*;
use log::{debug, info};
use object_store::aws::AmazonS3Builder;
use std::{collections::HashMap, env, sync::Arc};
use url::Url;

use crate::cli::Format;
use crate::deltalake::DeltaTable;
use crate::utils::ensure_scheme;

pub struct TableContext {
    ctx: SessionContext,
    path: Url,
    partition_spec: Option<Vec<(String, DataType)>>,
    fmt: Format,
}

impl TableContext {
    pub fn new(table_path: &str, partitions: &Option<String>, fmt: Format) -> Self {
        Self {
            ctx: SessionContext::new_with_config(
                SessionConfig::default().with_information_schema(true),
            ),
            path: ensure_scheme(table_path).unwrap(),
            partition_spec: get_partitions_spec(partitions),
            fmt: fmt,
        }
    }

    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    pub async fn register_table(&self) -> Result<()> {
        debug!("register table");
        let provider: Arc<dyn TableProvider> = match self.fmt {
            Format::Parquet => {
                let parquet_table = self.parquet_table_provider().await?;
                Arc::new(parquet_table)
            }
            Format::Delta => {
                let delta_table = self.delta_table_provider().await?;
                Arc::new(delta_table)
            }
        };
        self.ctx.register_table("tbl", provider)?;
        Ok(())
    }

    pub async fn schema(&self) -> Result<DataFrame> {
        let schema_query = "show columns from tbl";
        info!("schema query: {}", schema_query);
        Ok(self.ctx.sql(schema_query).await?)
    }

    pub fn build_query(&self, query: String, limit: usize) -> String {
        let full_query = if query.starts_with("SELECT") || query.starts_with("select") {
            format!("{} LIMIT {}", query, limit)
        } else {
            query.clone()
        };
        info!("full query: {}", full_query);
        full_query
    }

    pub async fn exec_query(&self, query: String, limit: usize) -> Result<DataFrame> {
        let full_query = self.build_query(query, limit);
        Ok(self.ctx.sql(full_query.as_str()).await?)
    }

    async fn parquet_table_provider(&self) -> Result<ListingTable> {
        debug!("register store");
        let url = &(self.path);
        match self.path.scheme() {
            "s3" | "s3a" => {
                let s3 = AmazonS3Builder::from_env()
                    .with_bucket_name(
                        url.host_str()
                            .expect("failed to extract host/bucket from path"),
                    )
                    .build()
                    .expect("Unable to create S3 object store");
                let s3_url =
                    Url::parse(&url[url::Position::BeforeScheme..url::Position::AfterHost])
                        .expect("Unable to get bucket based S3 url");
                let _ = self
                    .ctx
                    .runtime_env()
                    .object_store_registry
                    .register_store(&s3_url, Arc::new(s3));
            }
            _ => (),
        }
        debug!("get parquet table provider");
        let file_format = ParquetFormat::default()
            .with_enable_pruning(true)
            .with_skip_metadata(true);

        let listing_common_options =
            ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

        let listing_options = match self.partition_spec.clone() {
            Some(parts) => listing_common_options.with_table_partition_cols(parts),
            None => listing_common_options,
        };

        let path = ListingTableUrl::parse(self.path.as_str())?;
        let table_config = ListingTableConfig::new(path)
            .with_listing_options(listing_options)
            .infer_schema(&self.ctx.state())
            .await?;
        let table = ListingTable::try_new(table_config)?;
        Ok(table)
    }

    async fn delta_table_provider(&self) -> Result<DeltaTable> {
        debug!("get delta table provider");
        let mut storage_options = HashMap::new();
        match self.path.scheme() {
            "s3" | "s3a" => {
                for (os_key, os_value) in env::vars_os() {
                    if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                        if key.starts_with("AWS_") {
                            storage_options.insert(key.to_ascii_lowercase(), value.to_string());
                        }
                    }
                }
            }
            _ => (),
        }
        Ok(DeltaTable::from(format!("{}", self.path), storage_options)?)
    }
}

fn get_partitions_spec(partitions: &Option<String>) -> Option<Vec<(String, DataType)>> {
    if let Some(parts) = partitions.as_deref() {
        let mut vec = Vec::new();
        parts
            .split(',')
            .map(|s| s.trim())
            .map(|s| s.split(':').collect())
            .for_each(|t: Vec<&str>| {
                vec.push((t[0].to_string(), crate::utils::type_from_str(t[1]).unwrap()))
            });
        Some(vec)
    } else {
        None
    }
}
