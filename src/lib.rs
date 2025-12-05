//! Mishka shared library

#![warn(missing_docs)]
#![allow(clippy::style)]

#[cfg(feature = "cli")]
pub mod cli;
pub mod format;
pub mod polars;

#[derive(Debug)]
///User's input on file format
pub enum ExpectFormat {
    ///No formaat. Default value
    Infer,
    ///Expect CSV
    Csv,
    ///Expect Parquet
    Parquet,
}

impl ExpectFormat {
    ///Returns file format based on user's input
    ///
    ///If no specific file format is specified, attempts to infer from `path`
    ///
    ///Returns `None` if failed to determine file format from `path`
    pub fn select_or_infer(&self, path: &str) -> Option<crate::FileFormat> {
        match self {
            Self::Infer => {
                if path.ends_with("parquet") {
                    Some(crate::FileFormat::Parquet)
                } else if path.ends_with("csv") {
                    Some(crate::FileFormat::Csv)
                } else {
                    None
                }
            }
            Self::Csv => Some(crate::FileFormat::Csv),
            Self::Parquet => Some(crate::FileFormat::Parquet),
        }
    }
}

impl core::str::FromStr for ExpectFormat {
    type Err = &'static str;
    #[inline(always)]
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.eq_ignore_ascii_case("csv") {
            Ok(Self::Csv)
        } else if text.eq_ignore_ascii_case("parquet") {
            Ok(Self::Parquet)
        } else {
            Err("Invalid format. Allowed: 'csv' or 'parquet'")
        }
    }
}

///Available file format
pub enum FileFormat {
    ///CSV
    Csv,
    ///Parquet (including compressed)
    Parquet,
}

///Unique scan
pub struct Unique<CI: ExactSizeIterator<Item = String>> {
    ///Iterator of columns to select for uniquness
    pub columns: CI,
    ///Require to maintain order of the rows
    pub is_stable: bool,
}

///Sort by expression
pub struct SortBy {
    ///Field to sort by
    pub column: String,
    ///Indicates whether to sort in descending order or not
    pub desc: bool,
}

///Filters to query data
pub struct Query<CI: ExactSizeIterator<Item = String>, SBI: ExactSizeIterator<Item = SortBy>, UCI: ExactSizeIterator<Item = String> = CI> {
    ///Iterator of columns to select
    pub column: CI,
    ///Iterator over [SortBy]
    pub sort_by: SBI,
    ///If specified request deduplication data frames
    pub unique: Option<Unique<UCI>>,
}
