//!Polars module

use super::{cli, FileFormat, Query, SortBy, DUPLICATE_COLUMN};

pub use polars::error::PolarsError;
pub use polars::prelude::{Expr, PlRefPath, PlSmallStr};
pub use polars::prelude::{LazyCsvReader, LazyFileListReader, LazyFrame, col, lit};
pub use polars::prelude::{ScanArgsParquet, SortMultipleOptions, UniqueKeepStrategy};

impl<CI: ExactSizeIterator<Item = String>, SBI: ExactSizeIterator<Item = SortBy>, UCI: ExactSizeIterator<Item = String>, WHERE: ExactSizeIterator<Item = cli::Expression>> Query<CI, SBI, UCI, WHERE> {
    ///Scans `path` expecting specified `format`
    pub fn create_lazy_polars(self, path: &str, format: FileFormat, partition_by: &[String]) -> Result<LazyFrame, polars::error::PolarsError> {
        let mut df = match format {
            FileFormat::Csv => scan_csv(path)?,
            FileFormat::Parquet => scan_parquet(path, partition_by)?,
        };

        let mut select = Vec::new();
        let mut group_by = Vec::new();
        for column in self.column {
            select.push(col(column.as_str()));
            group_by.push(col(column));
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

        if !group_by.is_empty() {
            let agg_expr = col("*").count().alias(DUPLICATE_COLUMN);
            df = df.group_by(&group_by).agg([agg_expr]);
        }

        for filter in self.filter {
            let left = match filter.left {
                cli::Operand::Literal(literal) => lit(literal),
                cli::Operand::Identifier(ident) => col(ident),
            };

            let right = match filter.right {
                cli::Operand::Literal(literal) => lit(literal),
                cli::Operand::Identifier(ident) => col(ident),
            };

            let filter = match filter.operator {
                cli::Operator::Less => left.lt(right),
                cli::Operator::LessEq => left.lt_eq(right),
                cli::Operator::Eq => left.eq(right),
                cli::Operator::NotEq => left.neq(right),
                cli::Operator::GreaterEq => left.gt_eq(right),
                cli::Operator::Greater => left.gt(right),
            };
            df = df.filter(filter);
        }

        Ok(df)
    }
}

///Scan parquet through `path`
pub fn scan_parquet(path: &str, partition_by: &[String]) -> Result<LazyFrame, polars::error::PolarsError> {
    let uri = PlRefPath::new(path);

    let hive_options = if partition_by.is_empty() {
        polars::prelude::HiveOptions {
            enabled: None,
            ..Default::default()
        }
    } else {
        //TODO: consider if manual scheme specification would be needed
        polars::prelude::HiveOptions {
            enabled: Some(true),
            ..Default::default()
        }
    };

    let args = ScanArgsParquet {
        use_statistics: true,
        cache: true,
        glob: true,
        hive_options,
        allow_missing_columns: false,
        ..Default::default()
    };
    LazyFrame::scan_parquet(uri, args)
}

///Scan CSV through `path`
pub fn scan_csv(path: &str) -> Result<LazyFrame, polars::error::PolarsError> {
    use LazyFileListReader;

    let path = PlRefPath::new(path);
    LazyCsvReader::new(path)
        .with_glob(true)
        .with_cache(true)
        .with_has_header(true)
        .finish()
}
