#[cfg(all(feature = "adt-delta", feature = "delta"))]
compile_error!("feature \"adt-delta\" and feature \"delta\" cannot be enabled at the same time");

pub mod context;
pub mod error;
mod utils;
