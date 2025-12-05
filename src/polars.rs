//!Polars module

use super::{FileFormat, Query, SortBy};

pub use polars::error::PolarsError;
pub use polars::prelude::{Expr, PlPath, PlSmallStr};
pub use polars::prelude::{LazyCsvReader, LazyFileListReader, LazyFrame, col};
pub use polars::prelude::{ScanArgsParquet, SortMultipleOptions, UniqueKeepStrategy};

impl<CI: ExactSizeIterator<Item = String>, SBI: ExactSizeIterator<Item = SortBy>, UCI: ExactSizeIterator<Item = String>> Query<CI, SBI, UCI>
{
    ///Scans `path` expecting specified `format`
    pub fn create_lazy_polars(self, path: &str, format: FileFormat) -> Result<LazyFrame, polars::error::PolarsError> {
        let mut df = match format {
            FileFormat::Csv => scan_csv(path)?,
            FileFormat::Parquet => scan_parquet(path)?,
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
                columns.push(Expr::Column(sort_by.column.into()));
                descending.push(sort_by.desc)
            }

            let options = SortMultipleOptions {
                descending,
                ..Default::default()
            };
            //Figure out how sort supposed to work (it doesn't right now)
            df = df.sort_by_exprs(&columns, options)
        }

        if let Some(unique) = self.unique {
            let subset = (unique.columns.len() != 0).then(|| polars::lazy::dsl::Selector::ByName {
                //Next polars version will change it into array of Expr
                names: unique.columns.map(Into::into).collect::<Vec<_>>().into(),
                strict: true,
            });

            let strategy = UniqueKeepStrategy::Any;
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
pub fn scan_parquet(path: &str) -> Result<LazyFrame, polars::error::PolarsError> {
    let uri = PlPath::new(path);
    let args = ScanArgsParquet {
        use_statistics: true,
        cache: true,
        glob: true,
        allow_missing_columns: false,
        ..Default::default()
    };
    LazyFrame::scan_parquet(uri, args)
}

///Scan CSV through `path`
pub fn scan_csv(path: &str) -> Result<LazyFrame, polars::error::PolarsError> {
    use LazyFileListReader;

    let path = PlPath::new(path);
    LazyCsvReader::new(path)
        .with_glob(true)
        .with_cache(true)
        .with_has_header(true)
        .finish()
}
