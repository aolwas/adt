use datafusion::common::error::DataFusionError;
use snafu::prelude::*;

#[derive(Snafu, Debug)]
pub enum AdtError {
    #[snafu(context(false), display("Datafusion error"))]
    Datafusion { source: DataFusionError },
}
