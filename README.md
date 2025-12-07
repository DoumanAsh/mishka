# mishka

[![Rust](https://github.com/DoumanAsh/mishka/actions/workflows/rust.yml/badge.svg)](https://github.com/DoumanAsh/mishka/actions/workflows/rust.yml)
[![Crates.io](https://img.shields.io/crates/v/mishka.svg)](https://crates.io/crates/mishka)
[![Documentation](https://docs.rs/mishka/badge.svg)](https://docs.rs/crate/mishka/)

Utility to work with data files using [polars](https://crates.io/crates/polars) or [datafusion](https://github.com/apache/datafusion)

Mostly written to evaluate both Rust libraries.

## Common Usage

Common

```
mishaka 1.0.0-beta.2

Utility to work with data files

USAGE: [OPTIONS] <command>

OPTIONS:
    -h,  --help                      Prints this help information
         --backend <backend>         Specifies backend to use. Defaults to polars
         --select <select>...        List of column names to select
         --sort <sort>...            List of column names to sort in order
         --sort_desc                 Specifies descending order for sort. Defaults to ascending.
         --unique                    Specify to select unique
         --unique_by <unique_by>...  Specify columns to use to consider for uniqueness
         --stable                    Specify to use stable operations
         --format <format>           Expected file format. Defaults to inferring from path

ARGS:
    <command>  Command to run. Possible values: query, concat
```

## Query

Performs query only, outputting data to the console in loose CSV format

```
query: Query data

USAGE: [OPTIONS] <path>

OPTIONS:
    -h,  --help                 Prints this help information
         --chunk_by <chunk_by>  Limit number of elements to process at most. Default size 1000

ARGS:
    <path>  Path(s) to a file or directory (may be URI or include wildcard)
```

## Concat

Concatenates queried data into single output file

```
concat: Concatenates queried data into single file

USAGE: [OPTIONS] <path> <output>

OPTIONS:
    -h,  --help             Prints this help information
         --format <format>  Expected file format. Defaults to inferring from path

ARGS:
    <path>    Path(s) to a file or directory (may be URI or include wildcard)
    <output>  Path to a file to output (may be URI)
```
