use arrow::error::ArrowError;
use delta_kernel::Error as DKError;
use snafu::prelude::*;

#[derive(Snafu, Debug)]
pub enum DeltaError {
    #[snafu(context(false), display("DeltaKernel error"))]
    DeltaTable { source: DKError },
    #[snafu(context(false), display("Arrow error"))]
    Arrow { source: ArrowError },
}
