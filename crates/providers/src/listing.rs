// Original code: https://github.com/apache/datafusion/blob/main/datafusion/core/src/datasource/listing_table_factory.rs
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Factory for creating ListingTables with default options

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionState;

use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{DataFusionError, ToDFSchema, arrow_datafusion_err, plan_err};
use datafusion::common::{Result, config_datafusion_err};
use datafusion::logical_expr::CreateExternalTable;

use async_trait::async_trait;

/// A `TableProviderFactory` capable of creating new `ListingTable`s
#[derive(Debug, Default)]
pub struct ListingTableFactory {}

impl ListingTableFactory {
    /// Creates a new `ListingTableFactory`
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl TableProviderFactory for ListingTableFactory {
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        // TODO (https://github.com/apache/datafusion/issues/11600) remove downcast_ref from here. Should file format factory be an extension to session state?
        let session_state = state.as_any().downcast_ref::<SessionState>().unwrap();
        let file_format = session_state
            .get_file_format_factory(cmd.file_type.as_str())
            .ok_or(config_datafusion_err!(
                "Unable to create table with format {}! Could not find FileFormat.",
                cmd.file_type
            ))?
            .create(session_state, &cmd.options)?;

        let file_extension = get_extension(cmd.location.as_str(), file_format.get_ext().as_str());
        let (provided_schema, table_partition_cols) = if cmd.schema.fields().is_empty() {
            (
                None,
                cmd.table_partition_cols
                    .iter()
                    .map(|x| {
                        (
                            x.clone(),
                            DataType::Dictionary(
                                Box::new(DataType::UInt16),
                                Box::new(DataType::Utf8),
                            ),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        } else {
            let schema: SchemaRef = Arc::new(cmd.schema.as_ref().to_owned().into());
            let table_partition_cols = cmd
                .table_partition_cols
                .iter()
                .map(|col| {
                    schema
                        .field_with_name(col)
                        .map_err(|e| arrow_datafusion_err!(e))
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(|f| (f.name().to_owned(), f.data_type().to_owned()))
                .collect();
            // exclude partition columns to support creating partitioned external table
            // with a specified column definition like
            // `create external table a(c0 int, c1 int) stored as csv partitioned by (c1)...`
            let mut project_idx = Vec::new();
            for i in 0..schema.fields().len() {
                if !cmd.table_partition_cols.contains(schema.field(i).name()) {
                    project_idx.push(i);
                }
            }
            let schema = Arc::new(schema.project(&project_idx)?);
            (Some(schema), table_partition_cols)
        };

        let table_path = ListingTableUrl::parse(&cmd.location)?;

        let options = ListingOptions::new(file_format)
            .with_collect_stat(state.config().collect_statistics())
            .with_file_extension(file_extension)
            .with_target_partitions(state.config().target_partitions())
            .with_table_partition_cols(table_partition_cols);

        options
            .validate_partitions(session_state, &table_path)
            .await?;

        let resolved_schema = match provided_schema {
            // We will need to check the table columns against the schema
            // this is done so that we can do an ORDER BY for external table creation
            // specifically for parquet file format.
            // See: https://github.com/apache/datafusion/issues/7317
            None => {
                let schema = options.infer_schema(session_state, &table_path).await?;
                let df_schema = Arc::clone(&schema).to_dfschema()?;
                let column_refs: HashSet<_> = cmd
                    .order_exprs
                    .iter()
                    .flat_map(|sort| sort.iter())
                    .flat_map(|s| s.expr.column_refs())
                    .collect();

                for column in &column_refs {
                    if !df_schema.has_column(column) {
                        return plan_err!("Column {column} is not in schema");
                    }
                }

                schema
            }
            Some(s) => s,
        };

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options.with_file_sort_order(cmd.order_exprs.clone()))
            .with_schema(resolved_schema);
        let provider = ListingTable::try_new(config)?
            .with_cache(state.runtime_env().cache_manager.get_file_statistic_cache());
        let table = provider
            .with_definition(cmd.definition.clone())
            .with_constraints(cmd.constraints.clone())
            .with_column_defaults(cmd.column_defaults.clone());
        Ok(Arc::new(table))
    }
}

// Get file extension from path
fn get_extension(path: &str, default_ext: &str) -> String {
    let res = Path::new(path).extension().and_then(|ext| ext.to_str());
    format!(".{}", res.unwrap_or(default_ext))
}
