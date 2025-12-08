//!Datafusion module

use std::sync::Arc;

use super::{FileFormat, Query, SortBy};

pub use datafusion::dataframe::DataFrameWriteOptions;
pub use datafusion::execution::context::{SessionContext, SessionConfig};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
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
    pub async fn create_lazy_datafusion(self, mut ctx: SessionConfig, path: &str, format: FileFormat) -> Result<DataFrame, DataFusionError> {
        let env = create_runtime(path)?;
        if !self.coerce_int96.is_default() {
            ctx.options_mut().execution.parquet.coerce_int96 = Some(self.coerce_int96.as_unit_name().to_owned());
        }
        let ctx = SessionContext::new_with_config_rt(ctx, env);

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

#[cfg(any(feature = "aws", feature = "gcp"))]
struct BucketNameMissing;

#[cfg(any(feature = "aws", feature = "gcp"))]
impl core::fmt::Debug for BucketNameMissing {
    fn fmt(&self, fmt: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Display::fmt(self, fmt)
    }
}

#[cfg(any(feature = "aws", feature = "gcp"))]
impl core::fmt::Display for BucketNameMissing {
    fn fmt(&self, fmt: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        fmt.write_str("URI is missing bucket name")
    }
}

#[cfg(any(feature = "aws", feature = "gcp"))]
impl std::error::Error for BucketNameMissing {}

//Creates datafusion runtime based on hints from `path`
fn create_runtime(_path: &str) -> Result<Arc<RuntimeEnv>, DataFusionError> {
    //TODO: add memory limit using 50% of system memory
    let env = RuntimeEnvBuilder::new();

    #[cfg(feature = "aws")]
    if _path.starts_with("s3://") {
        let mut url: url::Url = _path.try_into().map_err(|error| DataFusionError::External(Box::new(error)))?;
        let bucket_name = match url.host_str() {
            Some(bucket_name) => bucket_name.to_owned(),
            None => return Err(DataFusionError::External(Box::new(BucketNameMissing))),
        };

        url.set_fragment(None);
        url.set_path("");
        url.set_query(None);

        println!(">Registering AWS storage with url={url}");
        let s3 = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket_name).build().map_err(|error| DataFusionError::External(Box::new(error)))?;
        env.object_store_registry.register_store(&url, Arc::new(s3));
    }

    #[cfg(feature = "gcp")]
    if _path.starts_with("gs://") {
        let mut url: url::Url = _path.try_into().map_err(|error| DataFusionError::External(Box::new(error)))?;
        let bucket_name = match url.host_str() {
            Some(bucket_name) => bucket_name.to_owned(),
            None => return Err(DataFusionError::External(Box::new(BucketNameMissing))),
        };

        url.set_fragment(None);
        url.set_path("");
        url.set_query(None);

        println!(">Registering GCP storage with url={url}");
        let gcp = object_store::gcp::GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket_name).build().map_err(|error| DataFusionError::External(Box::new(error)))?;
        env.object_store_registry.register_store(&url, Arc::new(gcp));
    }

    env.build_arc()
}
