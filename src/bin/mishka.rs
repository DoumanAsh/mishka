//!Executable entry point

#![allow(clippy::style)]

use std::process::ExitCode;

use mishka::cli;

macro_rules! error {
    ($($arg:tt)*) => {{
        eprintln!($($arg)*);
        return ExitCode::FAILURE
    }};
}

fn query(_args: cli::CommonArgs, _query: cli::Query) -> ExitCode {
    #[cfg(feature = "polars")]
    if _args.backend.is_polars() {
        return polars_query(_args, _query);
    }
    #[cfg(feature = "datafusion")]
    if _args.backend.is_datafusion() {
        return datafusion_query(_args, _query);
    }

    error!("No data processing backend is available")
}
fn concat(_args: cli::CommonArgs, _query: cli::Concat) -> ExitCode {
    #[cfg(feature = "polars")]
    if _args.backend.is_polars() {
        return polars_concat(_args, _query);
    }
    #[cfg(feature = "datafusion")]
    if _args.backend.is_datafusion() {
        return datafusion_concat(_args, _query);
    }

    error!("No data processing backend is available")
}

#[cfg(feature = "polars")]
fn polars_query(args: cli::CommonArgs, query: cli::Query) -> ExitCode {
    let format = match args.format.select_or_infer(&query.path) {
        Some(format) => format,
        None => error!("Unable to infer file format. Please specify --format"),
    };

    let df = match args.into_query().create_lazy_polars(&query.path, format) {
        Ok(df) => df.with_streaming(true),
        Err(error) => error!("{}: {error}", query.path.as_str()),
    };

    let (state, callback) = mishka::format::polars::batch_function();
    let df = match df.sink_batches(callback, false, core::num::NonZeroUsize::new(query.chunk_by)) {
        Ok(df) => df,
        Err(error) => error!("Unable to process data: {error}"),
    };

    match df.collect() {
        Ok(_) => println!("# Number of rows={}", state.row_count()),
        Err(error) => error!("Unable to collect data: {error}"),
    }

    ExitCode::SUCCESS
}

#[cfg(feature = "datafusion")]
fn datafusion_query(args: cli::CommonArgs, query: cli::Query) -> ExitCode {
    let rt = match tokio::runtime::Builder::new_current_thread().enable_time().enable_io().build() {
        Ok(rt) => rt,
        Err(error) => error!("Cannot initialize event loop: {error}"),
    };

    let format = match args.format.select_or_infer(&query.path) {
        Some(format) => format,
        None => error!("Unable to infer file format. Please specify --format"),
    };

    let mut cfg = mishka::datafusion::SessionConfig::new();
    if let Ok(chunk_by) = datafusion::config::ConfigNonZeroUsize::try_new(query.chunk_by) {
        cfg.options_mut().execution.batch_size = chunk_by;
    }
    rt.block_on(async move {
        let df = match args.into_query().create_lazy_datafusion(cfg, &query.path, format, &[]).await {
            Ok(df) => df,
            Err(error) => error!("{}: {error}", query.path)
        };

        let stream = match df.execute_stream_partitioned().await {
            Ok(stream) => stream,
            Err(error) => error!("Unable to process data: {error}"),
        };

        match mishka::format::datafusion::format_partitioned_data(stream).await {
            Ok(count) => println!("# Number of rows={count}"),
            Err(error) => error!("Unable to collect data: {error}"),
        }
        ExitCode::SUCCESS
    })
}

#[cfg(feature = "polars")]
fn polars_concat(args: cli::CommonArgs, query: cli::Concat) -> ExitCode {
    use core::fmt::Write;
    use polars::prelude::file_provider;

    let format = match args.format.select_or_infer(&query.path) {
        Some(format) => format,
        None => error!("Unable to infer file format. Please specify --format"),
    };
    let sink_format = match query.format.select_or_infer(&query.output) {
        Some(format) => format,
        None => error!("Unable to infer output format. Please specify --format"),
    };

    let df = match args.into_query().with_keep_partition(query.keep_partitions).create_lazy_polars(&query.path, format) {
        Ok(df) => df.with_streaming(true),
        Err(error) => error!("{}: {error}", query.path.as_str()),
    };

    let target = polars::prelude::PlRefPath::new(query.output.as_str());

    let destination = if query.partition_by.is_empty() {
        polars::prelude::SinkDestination::File {
            target: polars::prelude::SinkTarget::Path(target),
        }
    } else {
        let timestamp = mishka::utils::unit_now().as_secs();
        let mut prefix = query.prefix;
        if !prefix.is_empty() && !prefix.ends_with('-') {
            prefix.push('-');
        }
        let file_extension = sink_format.extension();
        let partition_by = query.partition_by.clone();
        let file_provider_cb = move |file_provider::FileProviderArgs { index_in_partition, partition_keys }: file_provider::FileProviderArgs| -> polars::prelude::PolarsResult<file_provider::FileProviderReturn> {
            //Despite its name, partition_keys contains values only
            //So align these values with query.partition_by list as partition_keys should be in the same order with column per value
            let mut result = String::new();
            for (idx, key) in partition_by.iter().enumerate() {
                let _ = match partition_keys.columns().get(idx).and_then(|column| column.get(0).ok()) {
                    Some(polars::prelude::AnyValue::Null) | None => write!(&mut result, "{key}=null/"),
                    //Special handling for strings to avoid default quotes in Display impl
                    Some(polars::prelude::AnyValue::String(value)) => write!(&mut result, "{key}={value}/"),
                    Some(polars::prelude::AnyValue::StringOwned(value)) => write!(&mut result, "{key}={value}/"),
                    //Other values can be formatted as it is
                    Some(value) => write!(&mut result, "{key}={value}/"),
                };
            }
            let _ = write!(&mut result, "{prefix}{timestamp}-{index_in_partition:03}.{file_extension}");
            Ok(file_provider::FileProviderReturn::Path(result))
        };
        let file_provider_cb = polars::prelude::PlanCallback::Rust(polars::prelude::SpecialEq::new(std::sync::Arc::new(file_provider_cb)));
        polars::prelude::SinkDestination::Partitioned {
            base_path: target,
            file_path_provider: Some(polars::prelude::file_provider::FileProviderType::Function(file_provider_cb)),
            partition_strategy: polars::prelude::PartitionStrategy::Keyed {
                keys: query.partition_by.into_iter().map(|col| polars::prelude::col(col)).collect(),
                include_keys: query.keep_partitions,
                keys_pre_grouped: true,
            },
            max_rows_per_file: 65_000,
            approximate_bytes_per_file: 2 * 1024 * 1024 * 1024
        }
    };

    let sink_options = polars::prelude::UnifiedSinkArgs {
        sync_on_close: polars::prelude::sync_on_close::SyncOnCloseType::Data,
        mkdir: true,
        ..Default::default()
    };

    let format = match sink_format {
        mishka::FileFormat::Csv => {
            let options = polars::prelude::CsvWriterOptions {
                include_header: true,
                ..Default::default()
            };
            polars::prelude::FileWriteFormat::Csv(options)
        }
        mishka::FileFormat::Parquet => {
            let options = polars::prelude::ParquetWriteOptions {
                compression: polars::prelude::ParquetCompression::Snappy,
                ..Default::default()
            };

            polars::prelude::FileWriteFormat::Parquet(std::sync::Arc::new(options))
        }
    };

    match df.sink(destination, format, sink_options) {
        Ok(df) => if let Err(error) = df.collect() {
            error!("Unable to collect data: {error}")
        },
        Err(error) => error!("{}: Unable to sink: {error}", query.output.as_str()),
    }

    ExitCode::SUCCESS
}

#[cfg(feature = "datafusion")]
fn datafusion_concat(args: cli::CommonArgs, query: cli::Concat) -> ExitCode {
    use core::fmt::Write;

    let rt = match tokio::runtime::Builder::new_current_thread().enable_time().enable_io().build() {
        Ok(rt) => rt,
        Err(error) => error!("Cannot initialize event loop: {error}"),
    };

    let format = match args.format.select_or_infer(&query.path) {
        Some(format) => format,
        None => error!("Unable to infer file format. Please specify --format"),
    };
    let sink_format = match query.format.select_or_infer(&query.output) {
        Some(format) => format,
        None => error!("Unable to infer output format. Please specify --format"),
    };

    let mut cfg = mishka::datafusion::SessionConfig::new();
    let timestamp = mishka::utils::unit_now().as_secs();

    let options = cfg.options_mut();
    options.execution.partitioned_file_prefix_name.clear();

    let prefix = query.prefix.trim();
    let _ = if prefix.is_empty() {
        write!(&mut options.execution.partitioned_file_prefix_name, "{timestamp}-")
    } else {
        write!(&mut options.execution.partitioned_file_prefix_name, "{prefix}-{timestamp}-")
    };

    rt.block_on(async move {
        let df_partition_by = if query.read_path_partitions {
            query.partition_by.as_slice()
        } else {
            &[]
        };

        let df = match args.into_query().with_keep_partition(query.keep_partitions).create_lazy_datafusion(cfg, &query.path, format, df_partition_by).await {
            Ok(result) => result,
            Err(error) => error!("{}: {error}", query.path)
        };

        let df_opts = if query.partition_by.is_empty() {
            mishka::datafusion::DataFrameWriteOptions::new().with_single_file_output(true)
        } else {
            mishka::datafusion::DataFrameWriteOptions::new().with_partition_by(query.partition_by)
        };

        match sink_format {
            mishka::FileFormat::Csv => {
                let csv_options = datafusion::config::CsvOptions {
                    has_header: Some(true),
                    ..Default::default()
                };

                if let Err(error) = df.write_csv(&query.output, df_opts, Some(csv_options)).await {
                    error!("{}: {error}", query.output)
                }
            },
            mishka::FileFormat::Parquet => {
                let parquet_options = datafusion::config::TableParquetOptions {
                    global: datafusion::config::ParquetOptions {
                        compression: Some("snappy".to_owned()),
                        coerce_int96: None,
                        //Minimize overhead of datafusion's arrow format
                        statistics_enabled: Some("none".to_owned()),
                        skip_arrow_metadata: true,
                        created_by: String::new(),
                        ..Default::default()
                    },
                    ..Default::default()
                };

                if let Err(error) = df.write_parquet(&query.output, df_opts, Some(parquet_options)).await {
                    error!("{}: {error}", query.output)
                }
            }
        }
        ExitCode::SUCCESS
    })
}

fn main() -> ExitCode {
    let (args, command) = cli::args().split_parts();
    match command {
        cli::Command::Query(params) => query(args, params),
        cli::Command::Concat(params) => concat(args, params),
    }
}
