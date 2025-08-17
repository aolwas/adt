use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::{DataFrame, SQLOptions, SessionConfig};
use deltalake::delta_datafusion::DeltaTableFactory;
use object_store;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;

#[cfg(feature = "adt-delta")]
use adt_providers::deltatable::DeltaTableFactory as NativeDeltaTableFactory;
use adt_providers::listing::ListingTableFactory;

use crate::error::AdtError;
use crate::utils::ensure_scheme;

pub struct ADTContext {
    ctx: SessionContext,
}

impl ADTContext {
    pub fn new() -> Self {
        let env = RuntimeEnvBuilder::new().build().unwrap();
        let ses = SessionConfig::new()
            .with_information_schema(true)
            .set_bool("datafusion.execution.parquet.pushdown_filters", true)
            .set_str("datafusion.sql_parser.dialect", "postgresql")
            .with_create_default_catalog_and_schema(true);

        let default_session_state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(ses)
            .with_runtime_env(Arc::new(env))
            // override default table factories to use patched ListingTableFactory
            .with_table_factory("PARQUET".into(), Arc::new(ListingTableFactory::new()))
            .with_table_factory("CSV".into(), Arc::new(ListingTableFactory::new()))
            .with_table_factory("JSON".into(), Arc::new(ListingTableFactory::new()))
            .with_table_factory("NDJSON".into(), Arc::new(ListingTableFactory::new()))
            .with_table_factory("AVRO".into(), Arc::new(ListingTableFactory::new()))
            .with_table_factory("ARROW".into(), Arc::new(ListingTableFactory::new()))
            .with_table_factory("DELTATABLE".into(), Arc::new(DeltaTableFactory {}));

        #[cfg(feature = "adt-delta")]
        let session_state = default_session_state.with_table_factory(
            "ADT_DELTATABLE".into(),
            Arc::new(NativeDeltaTableFactory::new()),
        );

        #[cfg(not(feature = "adt-delta"))]
        let session_state = default_session_state;

        Self {
            ctx: SessionContext::new_with_state(session_state.build()).enable_url_table(),
        }
    }

    fn register_object_store(&self, location: &String, file_type: &String) -> Result<(), AdtError> {
        let url = ensure_scheme(location).unwrap();
        match url.scheme() {
            "s3" | "s3a" => {
                let s3 = AmazonS3Builder::from_env()
                    .with_bucket_name(
                        url.host_str()
                            .expect("failed to extract host/bucket from path"),
                    )
                    .build()
                    .expect("Unable to create S3 object store");

                let _ = self
                    .ctx
                    .runtime_env()
                    .object_store_registry
                    .register_store(&url, Arc::new(s3));
                if file_type == "DELTATABLE" {
                    deltalake::aws::register_handlers(None);
                }
            }
            _ => (),
        }
        Ok(())
    }

    pub async fn execute_logical_plan(&self, plan: LogicalPlan) -> Result<DataFrame, AdtError> {
        if let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = &plan {
            println!("{:?}", cmd);
            self.register_object_store(&cmd.location, &cmd.file_type)?;
        }
        let df = self.ctx.execute_logical_plan(plan).await?;
        Ok(df)
    }

    pub async fn sql(&self, sql: &str) -> Result<DataFrame, AdtError> {
        self.sql_with_options(sql, SQLOptions::new()).await
    }

    pub async fn sql_with_options(
        &self,
        sql: &str,
        options: SQLOptions,
    ) -> Result<DataFrame, AdtError> {
        let plan = self.ctx.state().create_logical_plan(sql).await?;
        options.verify_plan(&plan)?;
        self.execute_logical_plan(plan).await
    }
}
