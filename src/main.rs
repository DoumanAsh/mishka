//!Executable entry point

#![allow(clippy::style)]

use std::process::ExitCode;

mod cli;

macro_rules! error {
    ($($arg:tt)*) => {{
        eprintln!($($arg)*);
        return ExitCode::FAILURE
    }};
}

fn query(args: cli::CommonArgs, query: cli::Query) -> ExitCode {
    let format = match args.format {
        cli::Format::Infer => {
            if query.path.0.ends_with("parquet") {
                mishka::FileFormat::Parquet
            } else if query.path.0.ends_with("csv") {
                mishka::FileFormat::Csv
            } else {
                error!("Unable to infer file format. Please specify --format")
            }
        },
        cli::Format::Csv => mishka::FileFormat::Csv,
        cli::Format::Parquet => mishka::FileFormat::Parquet,
    };

    let df = match args.into_query().create_lazy(&query.path.0, format) {
        Ok(df) => df.with_new_streaming(true),
        Err(error) => error!("{}: {error}", query.path.0.as_str())
    };

    let (state, callback) = mishka::format::batch_function();
    let df = match df.sink_batches(callback, false, core::num::NonZeroUsize::new(query.chunk_by)) {
        Ok(df) => df,
        Err(error) => error!("Unable to process data: {error}"),
    };

    match df.collect() {
        Ok(_) => println!("# Number of rows={}", state.row_count()),
        Err(error) => error!("Unable to collect data: {error}")
    }

    ExitCode::SUCCESS
}

fn main() -> ExitCode {
    let (args, command) = cli::args().split_parts();
    match command {
        cli::Command::Query(params) => query(args, params)
    }
}
