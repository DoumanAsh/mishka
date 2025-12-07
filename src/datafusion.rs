//!Datafusion module

use super::{FileFormat, Query, SortBy};

pub use datafusion::dataframe::DataFrameWriteOptions;
pub use datafusion::execution::context::{SessionContext, SessionConfig};
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::datasource::file_format::options::{CsvReadOptions, ParquetReadOptions};
use datafusion::logical_expr::col;
use datafusion::logical_expr::SortExpr;

fn apply_select_sort_on_non_distinct_query(mut df: DataFrame, select: impl ExactSizeIterator<Item = String>, sort: impl ExactSizeIterator<Item = SortBy>) -> Result<DataFrame, DataFusionError> {
    if select.len() > 0 {
        df = df.select(select.map(|column| col(column)))?
    };

    if sort.len() != 0 {
        //Stable sort?
        df.sort_by(sort.map(|sort| col(sort.column)).collect())
    } else {
        Ok(df)
    }
}

impl<CI: ExactSizeIterator<Item = String>, SBI: ExactSizeIterator<Item = SortBy>, UCI: ExactSizeIterator<Item = String>> Query<CI, SBI, UCI> {
    ///Scans `path` expecting specified `format`
    pub async fn create_lazy_datafusion(self, ctx: SessionConfig, path: &str, format: FileFormat) -> Result<DataFrame, DataFusionError> {
        let ctx = SessionContext::new_with_config(ctx);
        let mut df = match format {
            FileFormat::Csv => ctx.read_csv(path, CsvReadOptions::new().has_header(true)).await,
            FileFormat::Parquet => ctx.read_parquet(path, ParquetReadOptions::new()).await,
        }?;

        df = if let Some(unique) = self.unique {
            if unique.columns.len() == 0 {
                let distinct_on = unique.columns.map(|column| col(column)).collect();
                let select = self.column.map(|column| col(column)).collect();
                let sort = if self.sort_by.len() > 0 {
                    Some(self.sort_by.map(|it| SortExpr {
                        expr: col(it.column),
                        asc: !it.desc,
                        nulls_first: false
                    }).collect())
                } else {
                    None
                };
                df.distinct_on(distinct_on, select, sort)?
            } else {
                df = df.distinct()?;
                apply_select_sort_on_non_distinct_query(df, self.column, self.sort_by)?
            }
        } else {
            apply_select_sort_on_non_distinct_query(df, self.column, self.sort_by)?
        };

        Ok(df)
    }
}
