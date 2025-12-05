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

#[cfg(not(feature = "polars"))]
fn query(_args: cli::CommonArgs, _query: cli::Query) -> ExitCode {
    error!("No data processing backend is selected")
}
#[cfg(not(feature = "polars"))]
fn concat(_args: cli::CommonArgs, _query: cli::Concat) -> ExitCode {
    error!("No data processing backend is selected")
}

#[cfg(feature = "polars")]
fn query(args: cli::CommonArgs, query: cli::Query) -> ExitCode {
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

#[cfg(feature = "polars")]
fn concat(args: cli::CommonArgs, query: cli::Concat) -> ExitCode {
    let format = match args.format.select_or_infer(&query.path) {
        Some(format) => format,
        None => error!("Unable to infer file format. Please specify --format"),
    };
    let sink_format = match query.format.select_or_infer(&query.path) {
        Some(format) => format,
        None => error!("Unable to infer output format. Please specify --format"),
    };

    let mut df = match args.into_query().create_lazy_polars(&query.path, format) {
        Ok(df) => df.with_new_streaming(true),
        Err(error) => error!("{}: {error}", query.path.as_str()),
    };

    let target = polars::prelude::SinkTarget::Path(polars::prelude::PlPath::new(query.output.as_str()));
    let sink_options = polars::prelude::SinkOptions {
        sync_on_close: polars::prelude::sync_on_close::SyncOnCloseType::Data,
        ..Default::default()
    };
    df = match sink_format {
        mishka::FileFormat::Csv => {
            let options = polars::prelude::CsvWriterOptions {
                include_header: true,
                ..Default::default()
            };
            match df.sink_csv(target, options, None, sink_options) {
                Ok(df) => df,
                Err(error) => error!("{}: Unable to sink: {error}", query.output.as_str()),
            }
        }
        mishka::FileFormat::Parquet => {
            let options = polars::prelude::ParquetWriteOptions {
                compression: polars::prelude::ParquetCompression::Snappy,
                ..Default::default()
            };

            match df.sink_parquet(target, options, None, sink_options) {
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

fn main() -> ExitCode {
    let (args, command) = cli::args().split_parts();
    match command {
        cli::Command::Query(params) => query(args, params),
        cli::Command::Concat(params) => concat(args, params),
    }
}
