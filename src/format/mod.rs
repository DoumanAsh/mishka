//!Formatting

#[cfg(feature = "datafusion")]
pub mod datafusion;
#[cfg(feature = "polars")]
pub mod polars;

///Formatter for schema
pub struct Schema<'a, T>(pub &'a T);

///Formatter for data frame
pub struct DataFrame<'a, T>(pub &'a T);
