//!Datafusion module

use std::path::Path;
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
use datafusion::common::arrow::datatypes::DataType;

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
    pub async fn create_lazy_datafusion(self, mut ctx: SessionConfig, path: &str, format: FileFormat, partition_by: &[String]) -> Result<DataFrame, DataFusionError> {
        let mut user_partitions = partition_by.iter().map(String::as_str).collect::<Vec<_>>();

        let mut partition_filter: Option<datafusion::prelude::Expr> = None;
        let mut table_partition_cols = Vec::new();
        let mut table_path = String::new();
        let os_path = Path::new(path).to_owned();
        let is_file = os_path.extension().is_some();
        for component in os_path.iter().flat_map(|component| component.to_str()) {
            if let Some((key, value)) = component.split_once('=') {
                table_partition_cols.push((key.to_owned(), DataType::Utf8View));
                //Preserve order to make sure we pass partitions in the same order as user specified
                if let Some(idx) = user_partitions.iter().position(|val| *val == key) {
                    user_partitions.remove(idx);
                }
                if let Some(filter) = partition_filter.take() {
                    partition_filter = Some(filter.and(col(key).eq(datafusion::prelude::Expr::Literal(value.into(), None))));
                } else {
                    partition_filter = Some(col(key).eq(datafusion::prelude::Expr::Literal(value.into(), None)));
                }
            } else if table_partition_cols.is_empty() {
                table_path.push_str(component);
                table_path.push('/');
                if component.ends_with(':') {
                    table_path.push('/');
                }
            }
        }

        if is_file {
            table_path.pop();
        }

        if !table_partition_cols.is_empty() {
            println!(">Infer path partitions={:?}", table_partition_cols);
            println!(">Table path={table_path}");
        }
        //Assume user passes partitions in the same order as they should be in target
        //But we always exclude partitions contained in path
        for user_partition in user_partitions {
            table_partition_cols.push((user_partition.to_owned(), DataType::Utf8View));
        }


        {
            let options = ctx.options_mut();
            options.execution.keep_partition_by_columns = self.keep_partition;
            options.execution.listing_table_ignore_subdirectory = false;
            options.execution.listing_table_factory_infer_partitions = true;
            if !self.coerce_int96.is_default() {
                options.execution.parquet.coerce_int96 = Some(self.coerce_int96.as_unit_name().to_owned());
            }
        }

        let env = create_runtime(&table_path).await?;
        let ctx = SessionContext::new_with_config_rt(ctx, env);

        let mut df = match format {
            FileFormat::Csv => ctx.read_csv(table_path, CsvReadOptions::new().has_header(true).table_partition_cols(table_partition_cols)).await,
            FileFormat::Parquet => ctx.read_parquet(table_path, ParquetReadOptions::new().table_partition_cols(table_partition_cols)).await,
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

        if let Some(filter) = partition_filter {
            Ok(df.filter(filter)?)
        } else {
            Ok(df)
        }
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
async fn create_runtime(_path: &str) -> Result<Arc<RuntimeEnv>, DataFusionError> {
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

        match object_store_aws::init(Some(&object_store_aws::http::Builder::new().with_ring())).await {
            Ok(credentials) => {
                println!(">Registering AWS storage with url={url}");

                let mut s3 = object_store_aws::AmazonS3Builder::from_env().with_region(credentials.region_str()).with_bucket_name(bucket_name);
                match credentials.http_client() {
                    Ok(Some(http_client)) => {
                        s3 = s3.with_http_connector(http_client);
                    },
                    Ok(None) => {
                        println!("> AWS SDK HTTP client is not availalble");
                    },
                    Err(error) => {
                        eprintln!("# AWS SDK HTTP client is not availalble: {error}");
                    }
                }
                let s3 = s3.with_credentials(Arc::new(credentials)).build().map_err(|error| DataFusionError::External(Box::new(error)))?;
                env.object_store_registry.register_store(&url, Arc::new(s3));
            },
            Err(error) => return Err(DataFusionError::External(Box::new(error))),
        }
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
