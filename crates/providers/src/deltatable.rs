/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Original version:
https://github.com/spiceai/spiceai/blob/10221b20cca78eb7be9b649aea11dbc9e4f2d44b/crates/data_components/src/delta_lake.rs
*/

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::common::DFSchema;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::{
    DefaultParquetFileReaderFactory, ParquetAccessPlan, RowGroupAccess,
};
use datafusion::datasource::physical_plan::{
    FileScanConfig, ParquetExec, ParquetFileReaderFactory,
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, lit};
use datafusion::parquet::arrow::arrow_reader::RowSelection;
use datafusion::parquet::file::metadata::RowGroupMetaData;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::scalar::ScalarValue;
use delta_kernel::Table;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::scan::ScanBuilder;
use delta_kernel::scan::state::{DvInfo, GlobalScanState, Stats};
use delta_kernel::snapshot::Snapshot;
use log::debug;
use std::{collections::HashMap, sync::Arc};
use url::Url;

use crate::error::DeltaError;

type Result<T, E = DeltaError> = std::result::Result<T, E>;

#[derive(Debug, Default)]
pub struct DeltaTableFactory {}

impl DeltaTableFactory {
    /// Creates a new `DeltaTableFactory`
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl TableProviderFactory for DeltaTableFactory {
    async fn create(
        &self,
        _ctx: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let provider = if cmd.options.is_empty() {
            DeltaTable::from(cmd.to_owned().location, HashMap::new())
        } else {
            DeltaTable::from(cmd.to_owned().location, cmd.to_owned().options)
        };
        Ok(Arc::new(provider.unwrap()))
    }
}

#[derive(Debug)]
pub struct DeltaTable {
    table: Table,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    arrow_schema: SchemaRef,
    arrow_file_schema: SchemaRef,
    arrow_partition_cols: Arc<Vec<Field>>,
    delta_schema: delta_kernel::schema::SchemaRef,
}

impl DeltaTable {
    pub fn from(
        table_location: String,
        storage_options: HashMap<String, String>,
        // storage_options: HashMap<String, SecretString>,
    ) -> Result<Self> {
        let table = Table::try_from_uri(ensure_folder_location(table_location.clone()))?;

        let engine = Arc::new(DefaultEngine::try_new(
            table.location(),
            storage_options,
            Arc::new(TokioBackgroundExecutor::new()),
        )?);

        let snapshot = table.snapshot(engine.as_ref(), None)?;

        let arrow_schema = Arc::new(Self::get_schema(&snapshot));
        let arrow_file_schema = Arc::new(Self::get_file_schema(&snapshot));
        let arrow_partition_cols = Arc::new(Self::get_partition_schema(&snapshot));
        let delta_schema = Arc::new(snapshot.schema().clone());

        Ok(Self {
            table,
            engine,
            arrow_schema: arrow_schema,
            arrow_file_schema: arrow_file_schema,
            arrow_partition_cols: arrow_partition_cols,
            delta_schema: delta_schema,
        })
    }

    fn get_schema(snapshot: &Snapshot) -> Schema {
        let schema = snapshot.schema();

        // add partition columns at the end of the schema
        let fields: Vec<Field> = schema
            .fields()
            .map(|f| {
                Field::new(
                    f.name(),
                    map_delta_data_type_to_arrow_data_type(&f.data_type),
                    f.nullable,
                )
            })
            .collect();

        Schema::new(fields)
    }

    fn get_file_schema(snapshot: &Snapshot) -> Schema {
        let schema = snapshot.schema();
        let table_partition_cols = &snapshot.metadata().partition_columns;

        // add partition columns at the end of the schema
        let fields: Vec<Field> = schema
            .fields()
            .filter(|f| !table_partition_cols.contains(f.name()))
            .map(|f| {
                Field::new(
                    f.name(),
                    map_delta_data_type_to_arrow_data_type(&f.data_type),
                    f.nullable,
                )
            })
            .collect();

        Schema::new(fields)
    }

    fn get_partition_schema(snapshot: &Snapshot) -> Vec<Field> {
        let schema = snapshot.schema();
        let table_partition_cols = &snapshot.metadata().partition_columns;

        let mut fields: Vec<Field> = vec![];
        for partition_col in table_partition_cols.iter() {
            let dk_field = schema.field(partition_col).unwrap();
            fields.push(Field::new(
                dk_field.name(),
                map_delta_data_type_to_arrow_data_type(&dk_field.data_type),
                dk_field.nullable,
            ));
        }

        fields
    }
}

fn ensure_folder_location(table_location: String) -> String {
    if table_location.ends_with('/') {
        table_location
    } else {
        format!("{table_location}/")
    }
}

#[allow(clippy::cast_possible_wrap)]
fn map_delta_data_type_to_arrow_data_type(
    delta_data_type: &delta_kernel::schema::DataType,
) -> DataType {
    match delta_data_type {
        delta_kernel::schema::DataType::Primitive(primitive_type) => match primitive_type {
            delta_kernel::schema::PrimitiveType::String => DataType::Utf8,
            delta_kernel::schema::PrimitiveType::Long => DataType::Int64,
            delta_kernel::schema::PrimitiveType::Integer => DataType::Int32,
            delta_kernel::schema::PrimitiveType::Short => DataType::Int16,
            delta_kernel::schema::PrimitiveType::Byte => DataType::Int8,
            delta_kernel::schema::PrimitiveType::Float => DataType::Float32,
            delta_kernel::schema::PrimitiveType::Double => DataType::Float64,
            delta_kernel::schema::PrimitiveType::Boolean => DataType::Boolean,
            delta_kernel::schema::PrimitiveType::Binary => DataType::Binary,
            delta_kernel::schema::PrimitiveType::Date => DataType::Date32,
            delta_kernel::schema::PrimitiveType::Timestamp => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            delta_kernel::schema::PrimitiveType::TimestampNtz => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            delta_kernel::schema::PrimitiveType::Decimal(p, s) => {
                DataType::Decimal128(*p, *s as i8)
            }
        },
        delta_kernel::schema::DataType::Array(array_type) => DataType::List(Arc::new(Field::new(
            "item",
            map_delta_data_type_to_arrow_data_type(array_type.element_type()),
            array_type.contains_null(),
        ))),
        delta_kernel::schema::DataType::Struct(struct_type) => {
            let mut fields: Vec<Field> = vec![];
            for field in struct_type.fields() {
                fields.push(Field::new(
                    field.name(),
                    map_delta_data_type_to_arrow_data_type(field.data_type()),
                    field.nullable,
                ));
            }
            DataType::Struct(fields.into())
        }
        delta_kernel::schema::DataType::Map(map_type) => {
            let key_field = Arc::new(Field::new(
                "key",
                map_delta_data_type_to_arrow_data_type(map_type.key_type()),
                false,
            ));
            let value_field = Arc::new(Field::new(
                "value",
                map_delta_data_type_to_arrow_data_type(map_type.value_type()),
                true,
            ));
            DataType::Map(
                Arc::new(Field::new_struct(
                    "key_value",
                    [key_field, value_field],
                    false,
                )),
                false,
            )
        }
    }
}

#[async_trait]
impl TableProvider for DeltaTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.arrow_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, datafusion::error::DataFusionError> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, datafusion::error::DataFusionError> {
        let snapshot = self
            .table
            .snapshot(self.engine.as_ref(), None)
            .map_err(map_delta_error_to_datafusion_err)?;

        let df_schema = DFSchema::try_from(Arc::clone(&self.arrow_schema))?;
        let filter = conjunction(filters.to_vec()).unwrap_or_else(|| lit(true));
        let physical_expr = state.create_physical_expr(filter, &df_schema)?;

        let store = self
            .engine
            .get_object_store_for_url(self.table.location())
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "Failed to get object store for table location".to_string(),
                )
            })?;

        let parquet_file_reader_factory = Arc::new(DefaultParquetFileReaderFactory::new(store))
            as Arc<dyn ParquetFileReaderFactory>;
        let projected_delta_schema = project_delta_schema(
            &self.arrow_schema,
            Arc::clone(&self.delta_schema),
            projection,
        );

        let scan = ScanBuilder::new(Arc::new(snapshot))
            .with_schema(projected_delta_schema)
            .build()
            .map_err(map_delta_error_to_datafusion_err)?;
        let engine = Arc::clone(&self.engine);
        let scan_state = scan.global_scan_state();

        let mut scan_context = ScanContext::new(scan_state, Arc::clone(&self.engine));

        let scan_iter = scan
            .scan_data(engine.as_ref())
            .map_err(map_delta_error_to_datafusion_err)?;

        for scan_result in scan_iter {
            let data = scan_result.map_err(map_delta_error_to_datafusion_err)?;
            scan_context = delta_kernel::scan::state::visit_scan_files(
                data.0.as_ref(),
                data.1.as_ref(),
                scan_context,
                handle_scan_file,
            )
            .map_err(map_delta_error_to_datafusion_err)?;
        }

        if let Some(err) = scan_context.errs.into_iter().next() {
            return Err(err);
        }

        let mut partitioned_files: Vec<PartitionedFile> = vec![];
        for file in scan_context.files {
            let mut partitioned_file = file.partitioned_file;

            // If there is a selection vector, create a ParquetAccessPlan that will be used to skip rows based on the selection vector
            if let Some(selection_vector) = file.selection_vector {
                let access_plan = get_parquet_access_plan(
                    &parquet_file_reader_factory,
                    &partitioned_file,
                    selection_vector,
                )
                .await?;
                partitioned_file = partitioned_file.with_extensions(Arc::new(access_plan));
            }

            partitioned_files.push(partitioned_file);
        }

        // FileScanConfig requires an ObjectStoreUrl, but it isn't actually used because we pass in a ParquetFileReaderFactory
        // which specifies which object store to read from.
        let file_scan_config = FileScanConfig::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::clone(&self.arrow_file_schema),
        )
        .with_limit(limit)
        .with_projection(projection.cloned())
        .with_file_group(partitioned_files)
        .with_table_partition_cols((*self.arrow_partition_cols).clone());
        let exec = ParquetExec::builder(file_scan_config)
            .with_parquet_file_reader_factory(Arc::clone(&parquet_file_reader_factory))
            .with_predicate(Arc::clone(&physical_expr))
            .build();

        Ok(Arc::new(exec))
    }
}

struct ScanContext {
    pub errs: Vec<datafusion::error::DataFusionError>,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    scan_state: GlobalScanState,
    pub files: Vec<PartitionFileContext>,
}

impl ScanContext {
    fn new(
        scan_state: GlobalScanState,
        engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    ) -> Self {
        Self {
            scan_state,
            engine,
            errs: Vec::new(),
            files: Vec::new(),
        }
    }
}

fn project_delta_schema(
    arrow_schema: &SchemaRef,
    schema: delta_kernel::schema::SchemaRef,
    projections: Option<&Vec<usize>>,
) -> delta_kernel::schema::SchemaRef {
    if let Some(projections) = projections {
        let projected_fields = projections
            .iter()
            .filter_map(|i| schema.field(arrow_schema.field(*i).name()))
            .cloned()
            .collect::<Vec<_>>();
        Arc::new(delta_kernel::schema::Schema::new(projected_fields))
    } else {
        schema
    }
}

struct PartitionFileContext {
    partitioned_file: PartitionedFile,
    selection_vector: Option<Vec<bool>>,
}

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::cast_sign_loss)]
fn handle_scan_file(
    scan_context: &mut ScanContext,
    path: &str,
    size: i64,
    _stats: Option<Stats>,
    dv_info: DvInfo,
    partition_values: HashMap<String, String>,
) {
    let root_url = match Url::parse(&scan_context.scan_state.table_root) {
        Ok(url) => url,
        Err(e) => {
            scan_context
                .errs
                .push(datafusion::error::DataFusionError::Execution(format!(
                    "Error parsing table root URL: {e}",
                )));
            return;
        }
    };
    let path = format!("{}/{path}", root_url.path());

    let mut partitioned_file = PartitionedFile::new(path.clone(), size as u64);

    let partition_values = scan_context
        .scan_state
        .partition_columns
        .iter()
        .map(|col| {
            ScalarValue::try_from_string(
                partition_values[col].clone(),
                &map_delta_data_type_to_arrow_data_type(
                    scan_context.scan_state.logical_schema.fields[col].data_type(),
                ),
            )
            .unwrap()
        })
        .collect::<Vec<ScalarValue>>();

    partitioned_file.partition_values = partition_values;

    // Get the selection vector (i.e. inverse deletion vector)
    let selection_vector =
        match dv_info.get_selection_vector(scan_context.engine.as_ref(), &root_url) {
            Ok(selection_vector) => selection_vector,
            Err(e) => {
                scan_context
                    .errs
                    .push(datafusion::error::DataFusionError::Execution(format!(
                        "Error getting selection vector: {e}",
                    )));
                return;
            }
        };

    scan_context.files.push(PartitionFileContext {
        partitioned_file,
        selection_vector,
    });
}

fn map_delta_error_to_datafusion_err(e: delta_kernel::Error) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}

fn get_row_group_access(
    selection_vector: &[bool],
    row_group_row_start: usize,
    row_group_num_rows: usize,
) -> RowGroupAccess {
    // If all rows in the row group are deleted (i.e. not selected), skip the row group
    if selection_vector[row_group_row_start..row_group_row_start + row_group_num_rows]
        .iter()
        .all(|&x| !x)
    {
        return RowGroupAccess::Skip;
    }
    // If all rows in the row group are present (i.e. selected), scan the full row group
    if selection_vector[row_group_row_start..row_group_row_start + row_group_num_rows]
        .iter()
        .all(|&x| x)
    {
        return RowGroupAccess::Scan;
    }

    let mask =
        selection_vector[row_group_row_start..row_group_row_start + row_group_num_rows].to_vec();

    // If some rows are deleted, get a row selection that skips the deleted rows
    let row_selection = RowSelection::from_filters(&[mask.into()]);
    RowGroupAccess::Selection(row_selection)
}

fn get_full_selection_vector(selection_vector: &[bool], total_rows: usize) -> Vec<bool> {
    let mut new_selection_vector = vec![true; total_rows];
    new_selection_vector[..selection_vector.len()].copy_from_slice(selection_vector);
    new_selection_vector
}

#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_sign_loss)]
async fn get_parquet_access_plan(
    parquet_file_reader_factory: &Arc<dyn ParquetFileReaderFactory>,
    partitioned_file: &PartitionedFile,
    selection_vector: Vec<bool>,
) -> Result<ParquetAccessPlan, datafusion::error::DataFusionError> {
    let mut parquet_file_reader = parquet_file_reader_factory.create_reader(
        0,
        partitioned_file.object_meta.clone().into(),
        None,
        &ExecutionPlanMetricsSet::new(),
    )?;

    let parquet_metadata = parquet_file_reader.get_metadata().await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Error getting parquet metadata: {e}"
        ))
    })?;

    let total_rows = parquet_metadata
        .row_groups()
        .iter()
        .map(RowGroupMetaData::num_rows)
        .sum::<i64>();

    let selection_vector = get_full_selection_vector(&selection_vector, total_rows as usize);

    // Create a ParquetAccessPlan that will be used to skip rows based on the selection vector
    let mut row_groups: Vec<RowGroupAccess> = vec![];
    let mut row_group_row_start = 0;
    for (i, row_group) in parquet_metadata.row_groups().iter().enumerate() {
        // If all rows in the row group are deleted, skip the row group
        debug!(
            "Row group {i} num_rows={} row_group_row_start={row_group_row_start}",
            row_group.num_rows()
        );
        let row_group_access = get_row_group_access(
            &selection_vector,
            row_group_row_start,
            row_group.num_rows() as usize,
        );
        row_groups.push(row_group_access);
        row_group_row_start += row_group.num_rows() as usize;
    }

    debug!("Created ParquetAccessPlan with {row_groups:?}");
    Ok(ParquetAccessPlan::new(row_groups))
}

#[cfg(test)]
mod tests {
    use datafusion::parquet::arrow::arrow_reader::RowSelector;

    use super::*;

    #[test]
    fn test_get_row_group_access() {
        let selection_vector = &[true, true, true, true, true];
        let row_group_row_start = 0;
        let row_group_num_rows = 5;
        let row_group_access =
            get_row_group_access(selection_vector, row_group_row_start, row_group_num_rows);

        assert_eq!(row_group_access, RowGroupAccess::Scan);

        let selection_vector = &[false, false, false, false, false];
        let row_group_row_start = 0;
        let row_group_num_rows = 5;
        let row_group_access =
            get_row_group_access(selection_vector, row_group_row_start, row_group_num_rows);

        assert_eq!(row_group_access, RowGroupAccess::Skip);

        let selection_vector = &[true, true, true, false, true];
        let row_group_row_start = 0;
        let row_group_num_rows = 5;
        let row_group_access =
            get_row_group_access(selection_vector, row_group_row_start, row_group_num_rows);

        let selectors = vec![
            RowSelector::select(3),
            RowSelector::skip(1),
            RowSelector::select(1),
        ];
        assert_eq!(
            row_group_access,
            RowGroupAccess::Selection(selectors.into())
        );
    }

    #[test]
    fn test_get_table_location() {
        assert_eq!(
            ensure_folder_location("s3://my_bucket/".to_string()),
            "s3://my_bucket/"
        );
        assert_eq!(
            ensure_folder_location("s3://my_bucket".to_string()),
            "s3://my_bucket/"
        );
    }
}
