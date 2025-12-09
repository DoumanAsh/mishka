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
        Ok(df) => df.with_new_streaming(true),
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
    cfg.options_mut().execution.batch_size = query.chunk_by;
    rt.block_on(async move {
        let df = match args.into_query().create_lazy_datafusion(cfg, &query.path, format).await {
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
    let format = match args.format.select_or_infer(&query.path) {
        Some(format) => format,
        None => error!("Unable to infer file format. Please specify --format"),
    };
    let sink_format = match query.format.select_or_infer(&query.output) {
        Some(format) => format,
        None => error!("Unable to infer output format. Please specify --format"),
    };

    let mut df = match args.into_query().create_lazy_polars(&query.path, format) {
        Ok(df) => df.with_new_streaming(true),
        Err(error) => error!("{}: {error}", query.path.as_str()),
    };

    let partition_variant = if query.partition_by.is_empty() {
        None
    } else {
        Some(polars::prelude::PartitionVariant::ByKey {
            key_exprs: query.partition_by.into_iter().map(|col| polars::prelude::col(col)).collect(),
            include_key: true
        })
    };

    let target = polars::prelude::PlPath::new(query.output.as_str());
    let sink_options = polars::prelude::SinkOptions {
        sync_on_close: polars::prelude::sync_on_close::SyncOnCloseType::Data,
        mkdir: true,
        ..Default::default()
    };

    df = match sink_format {
        mishka::FileFormat::Csv => {
            let options = polars::prelude::CsvWriterOptions {
                include_header: true,
                ..Default::default()
            };
            let result = match partition_variant {
                Some(variant) => df.sink_csv_partitioned(target.into(), None, variant, options, None, sink_options, None, None),
                None => df.sink_csv(polars::prelude::SinkTarget::Path(target), options, None, sink_options),
            };
            match result {
                Ok(df) => df,
                Err(error) => error!("{}: Unable to sink: {error}", query.output.as_str()),
            }
        }
        mishka::FileFormat::Parquet => {
            let options = polars::prelude::ParquetWriteOptions {
                compression: polars::prelude::ParquetCompression::Snappy,
                ..Default::default()
            };

            let result = match partition_variant {
                Some(variant) => df.sink_parquet_partitioned(target.into(), None, variant, options, None, sink_options, None, None),
                None => df.sink_parquet(polars::prelude::SinkTarget::Path(target), options, None, sink_options),
            };

            match result {
                Ok(df) => df,
                Err(error) => error!("{}: Unable to sink: {error}", query.output.as_str()),
            }
        }
    };

    if let Err(error) = df.collect() {
        error!("Unable to collect data: {error}")
    }

    ExitCode::SUCCESS
}

#[cfg(feature = "datafusion")]
fn datafusion_concat(args: cli::CommonArgs, query: cli::Concat) -> ExitCode {
    let df_opts = if query.partition_by.is_empty() {
        mishka::datafusion::DataFrameWriteOptions::new().with_single_file_output(true)
    } else {
        mishka::datafusion::DataFrameWriteOptions::new().with_partition_by(query.partition_by)
    };

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

    let cfg = mishka::datafusion::SessionConfig::new();
    rt.block_on(async move {
        let df = match args.into_query().create_lazy_datafusion(cfg, &query.path, format).await {
            Ok(result) => result,
            Err(error) => error!("{}: {error}", query.path)
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
