# mishka

[![Rust](https://github.com/DoumanAsh/mishka/actions/workflows/rust.yml/badge.svg)](https://github.com/DoumanAsh/mishka/actions/workflows/rust.yml)
[![Crates.io](https://img.shields.io/crates/v/mishka.svg)](https://crates.io/crates/mishka)
[![Documentation](https://docs.rs/mishka/badge.svg)](https://docs.rs/crate/mishka/)

Utility to work with data files using [polars](https://crates.io/crates/polars) or [datafusion](https://github.com/apache/datafusion)

Mostly written to evaluate both Rust libraries.

## Common Usage

Common

```
mishaka 1.0.0-beta.3

Utility to work with data files

USAGE: [OPTIONS] <command>

OPTIONS:
    -h,  --help                         Prints this help information
         --backend <backend>            Specifies backend to use. Defaults to polars
         --select <select>...           List of column names to select
         --sort <sort>...               List of column names to sort in order
         --sort_desc                    Specifies descending order for sort. Defaults to ascending.
         --unique                       Specify to select unique
         --unique_by <unique_by>...     Specify columns to use to consider for uniqueness
         --stable                       Specify to use stable operations
         --format <format>              Expected file format. Defaults to inferring from path
         --coerce_int96 <coerce_int96>  Specifies time unit for int96. Defaults to nanosecond

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

## Thoughts

Both libraries provide efficient way to stream data (no detailed performance/memory usage though) which should be sufficient for general use

There are some differences to consider:

### Polars

- Simple and easy to use API
- Because python API takes precedence, Rust API often is changing and experimental, high risk of breaking changes in future
- For the same reason API is not uniform, often functionally similar APIs would require completely different approach (see difference between reading parquet and CSV)
- Regular blocking calls without need of async runtime
- Can transform underlying data if not supposed (e.g. INT96)
- Not very well documented rust API, but python bindings are easy to navigate on how to use particular Rust APIs (in addition to having high level docs that show entry points to variety of methods)
- Heavier dependency wise so slower compile times but code size is as fat as datafusion, so not a big deal
- Hard-coded to write footer into parquet, resulting in output with overhead

### Datafusion

- A very 'boring' and complex API to navigate
- Once you understand API, it is very simple to use due to it being mostly uniform
- All APIs are async, so bootstrapping event loop is necessary (it is not very well documented what sort of tokio's features you need aka time/io)
- Allows to customize behavior when encountering weird stuff (e.g. how to coerce INT96)
    - No way to avoid transform underlying data if not supposed by Arrow (e.g. INT96)
- Not complete, but very thoughtful documentation straight in Rust API Docs
- Don't really care for SQL syntax, but it supports it I guess
    - On side note, a lot of APIs are named after SQL so that's nice
- Global context can be used to define all configuration parameters
    - Although it has some shared state behind lock, it is probably more useful for property of being global config
    - I guess you can rely on shared state to optimize multiple queries? Not sure how useful it would be in practice
- AWS & GCP features of object_store require manual enabling which is not convenient comparing to `polars`
