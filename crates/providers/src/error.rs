use arrow::error::ArrowError;
#[cfg(feature = "delta")]
use delta_kernel::Error as DKError;
use snafu::prelude::*;

#[derive(Snafu, Debug)]
pub enum DeltaError {
    #[cfg(feature = "delta")]
    #[snafu(context(false), display("DeltaKernel error"))]
    DeltaTable { source: DKError },
    #[snafu(context(false), display("Arrow error"))]
    Arrow { source: ArrowError },
}
