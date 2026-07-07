//!Datafusion module

use std::path::Path;
use std::sync::Arc;

use super::{cli, FileFormat, Query, SortBy, DUPLICATE_COLUMN};

pub use datafusion::dataframe::DataFrameWriteOptions;
pub use datafusion::execution::context::{SessionContext, SessionConfig};
use datafusion::catalog::default_table_source::DefaultTableSource;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{col, lit};
use datafusion::logical_expr::{Expr, SortExpr};
use datafusion::common::arrow::datatypes::DataType;
use futures_util::StreamExt;

fn apply_select_sort_on_non_distinct_query(mut df: DataFrame, select: Vec<Expr>, sort: impl ExactSizeIterator<Item = SortBy>) -> Result<DataFrame, DataFusionError> {
    if select.len() > 0 {
        df = df.select(select)?
    };

    if sort.len() != 0 {
        //Stable sort?
        df.sort_by(sort.map(|sort| col(sort.column)).collect())
    } else {
        Ok(df)
    }
}

impl<CI: ExactSizeIterator<Item = String>, SBI: ExactSizeIterator<Item = SortBy>, UCI: ExactSizeIterator<Item = String>, WHERE: ExactSizeIterator<Item = cli::Expression>> Query<CI, SBI, UCI, WHERE> {
    ///Scans `path` expecting specified `format`
    pub async fn create_lazy_datafusion(self, mut ctx: SessionConfig, path: &str, format: FileFormat, partition_by: &[String]) -> Result<DataFrame, DataFusionError> {
        use datafusion::datasource::file_format;

        let mut user_partitions = partition_by.iter().map(String::as_str).collect::<Vec<_>>();


        let mut partition_filters = Vec::new();
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
                partition_filters.push(col(key).eq(datafusion::prelude::Expr::Literal(value.into(), None)));
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

        let original_path = datafusion::datasource::listing::ListingTableUrl::parse(path)?;
        let listing_path = datafusion::datasource::listing::ListingTableUrl::parse(&table_path)?;
        let listing_options = datafusion::datasource::listing::ListingOptions::new(match format {
            FileFormat::Csv => Arc::new(file_format::csv::CsvFormat::default().with_has_header(true)),
            FileFormat::Parquet => Arc::new(file_format::parquet::ParquetFormat::new()),
        }).with_table_partition_cols(table_partition_cols.clone());

        println!(">{original_path}: Fetching available file");
        let ctx_object_store = ctx.runtime_env().object_store(&original_path)?;
        let first_file = match original_path.list_all_files(&ctx.state(), &*ctx_object_store, &listing_options.file_extension).await?.next().await {
            Some(first_file) => first_file?,
            None => return Err(DataFusionError::Internal(format!("{path}: No files available to infer schema"))),
        };
        println!(">{}: Inferring schema", first_file.location);
        let schema = listing_options.format.infer_schema(&ctx.state(), &ctx_object_store, &[first_file]).await?;
        let config = datafusion::datasource::listing::ListingTableConfig::new(listing_path).with_listing_options(listing_options).with_schema(schema.clone());
        let listing = datafusion::datasource::listing::ListingTable::try_new(config)?;

        let table_name = table_path.trim_end_matches('/').rsplit('/').next().unwrap();
        if partition_filters.is_empty() {
            println!(">Read table '{table_name}'");
        } else {
            println!(">Read table '{table_name}' with filters {filter}", filter=crate::format::datafusion::FiltersFmt(&partition_filters));
        }
        let df_plan = datafusion::logical_expr::LogicalPlanBuilder::scan_with_filters(table_name, Arc::new(DefaultTableSource::new(Arc::new(listing))), None, partition_filters)?.build()?;
        let mut df = datafusion::dataframe::DataFrame::new(ctx.state(), df_plan);
        let select_columns = self.column.map(col).collect();
        df = if let Some(unique) = self.unique {
            if unique.columns.len() == 0 {
                let distinct_on = unique.columns.map(|column| col(column)).collect();
                let sort = if self.sort_by.len() > 0 {
                    Some(self.sort_by.map(|it| SortExpr {
                        expr: col(it.column),
                        asc: !it.desc,
                        nulls_first: false
                    }).collect())
                } else {
                    None
                };
                df.distinct_on(distinct_on, select_columns, sort)?
            } else {
                df = df.distinct()?;
                apply_select_sort_on_non_distinct_query(df, select_columns, self.sort_by)?
            }
        } else {
            if self.count_duplicates {
                let aggr_expr = vec![datafusion::functions_aggregate::count::count(lit("*")).alias(DUPLICATE_COLUMN)];
                let df = if select_columns.len() > 0 {
                    df.aggregate(select_columns.clone(), aggr_expr)?
                } else {
                    let group_expr = schema.fields().iter().map(|field| col(field.name())).collect();
                    df.aggregate(group_expr, aggr_expr)?
                };

                apply_select_sort_on_non_distinct_query(df, select_columns, self.sort_by)?
            } else {
                apply_select_sort_on_non_distinct_query(df, select_columns, self.sort_by)?
            }
        };

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
                cli::Operator::NotEq => left.not_eq(right),
                cli::Operator::GreaterEq => left.gt_eq(right),
                cli::Operator::Greater => left.gt(right),
            };
            df = df.filter(filter)?;
        }

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
async fn create_runtime(_path: &str) -> Result<Arc<RuntimeEnv>, DataFusionError> {
    //TODO: Current cache manager forces full table listing on cache miss, so disable file list
    //caching until it is fixed
    //
    //https://github.com/apache/datafusion/issues/23341
    let cache_config = CacheManagerConfig::default().with_list_files_cache(None).with_list_files_cache_limit(0);
    //TODO: add memory limit using 50% of system memory
    let env = RuntimeEnvBuilder::new().with_cache_manager(cache_config);

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
