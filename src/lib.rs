//! Mishka shared library

#![warn(missing_docs)]
#![allow(clippy::style)]

pub use polars::prelude::{DataFrame, LazyFrame};
pub use polars::datatypes::PlSmallStr;

pub mod format;

///Available file format
pub enum FileFormat {
    ///CSV
    Csv,
    ///Parquet (including compressed)
    Parquet
}

///Unique scan
pub struct Unique<CI: ExactSizeIterator<Item = PlSmallStr>> {
    ///Iterator of columns to select for uniquness
    pub columns: CI,
    ///Require to maintain order of the rows
    pub is_stable: bool
}

///Sort by expression
pub struct SortBy {
    ///Field to sort by
    pub column: PlSmallStr,
    ///Indicates whether to sort in descending order or not
    pub desc: bool
}

///Filters to query data
pub struct Query<CI: ExactSizeIterator<Item = PlSmallStr>, SBI: ExactSizeIterator<Item = SortBy>, UCI: ExactSizeIterator<Item = PlSmallStr> = CI> {
    ///Iterator of columns to select
    pub column: CI,
    ///Iterator over [SortBy]
    pub sort_by: SBI,
    ///If specified request deduplication data frames
    pub unique: Option<Unique<UCI>>,
}

impl<CI: ExactSizeIterator<Item = PlSmallStr>, SBI: ExactSizeIterator<Item = SortBy>, UCI: ExactSizeIterator<Item = PlSmallStr>> Query<CI, SBI, UCI> {
    ///Scans `path` expecting specified `format`
    pub fn create_lazy(self, path: &PlSmallStr, format: FileFormat) -> Result<LazyFrame, polars::error::PolarsError> {
        use polars::prelude::col;

        let mut df = match format {
            FileFormat::Csv => scan_csv(path)?,
            FileFormat::Parquet => scan_parquet(path)?
        };

        let mut select = Vec::new();
        for column in self.column {
            select.push(col(column));
        }
        if select.is_empty() {
            select.push(col(PlSmallStr::from_static("*")));
        }
        df = df.select(&select);

        if self.sort_by.len() != 0 {
            let mut descending = Vec::new();
            let mut columns = Vec::new();

            for sort_by in self.sort_by {
                columns.push(polars::prelude::Expr::Column(sort_by.column));
                descending.push(sort_by.desc)
            }

            let options = polars::prelude::SortMultipleOptions {
                descending,
                ..Default::default()
            };
            //Figure out how sort supposed to work (it doesn't right now)
            df = df.sort_by_exprs(&columns, options)
        }

        if let Some(unique) = self.unique {
            let subset = (unique.columns.len() != 0).then(|| polars::lazy::dsl::Selector::ByName {
                //Next polars version will change it into array of Expr
                names: unique.columns.collect::<Vec<_>>().into(),
                strict: true
            });
            let strategy = polars::prelude::UniqueKeepStrategy::Any;

            df = if unique.is_stable {
                df.unique_stable(subset, strategy)
            } else {
                df.unique(subset, strategy)
            };
        }

        Ok(df.select(&select))
    }
}

///Scan parquet through `path`
pub fn scan_parquet(path: &PlSmallStr) -> Result<LazyFrame, polars::error::PolarsError> {
    let uri = polars::prelude::PlPath::new(path.as_str());
    let args = polars::prelude::ScanArgsParquet {
        use_statistics: true,
        cache: true,
        glob: true,
        allow_missing_columns: false,
        ..Default::default()
    };
    LazyFrame::scan_parquet(uri, args)
}

///Scan CSV through `path`
pub fn scan_csv(path: &PlSmallStr) -> Result<LazyFrame, polars::error::PolarsError> {
    use polars::prelude::LazyFileListReader;

    let path = polars::prelude::PlPath::new(path.as_str());
    polars::prelude::LazyCsvReader::new(path).with_glob(true).with_cache(true).with_has_header(true).finish()
}
