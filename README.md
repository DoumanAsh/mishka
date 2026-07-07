# mishka

[![Rust](https://github.com/DoumanAsh/mishka/actions/workflows/rust.yml/badge.svg)](https://github.com/DoumanAsh/mishka/actions/workflows/rust.yml)
[![Crates.io](https://img.shields.io/crates/v/mishka.svg)](https://crates.io/crates/mishka)
[![Documentation](https://docs.rs/mishka/badge.svg)](https://docs.rs/crate/mishka/)

Utility to work with data files using [polars](https://crates.io/crates/polars) or [datafusion](https://github.com/apache/datafusion)

Mostly written to evaluate both Rust libraries.

## Common Usage

Common

```
mishka 1.0.0-beta.9
Utility to work with data files

USAGE: [OPTIONS] <command>

OPTIONS:
    -h,  --help                         Prints this help information
         --backend <backend>            Specifies backend to use. Defaults to datafusion. Can be set via env MISHKA_BACKEND
         --select <select>...           List of column names to select
         --sort <sort>...               List of column names to sort in order
         --sort_desc                    Specifies descending order for sort. Defaults to ascending.
         --unique                       Specify to select unique
         --unique_by <unique_by>...     Specify columns to use to consider for uniqueness
         --count_duplicates             Specify to count duplicate records under column `dup_count`
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
    -h,  --help                            Prints this help information
         --partition_by <partition_by>...  List of column names to partition by (in order)
         --keep_partitions                 Specifies to keep partitioned columns in output. By default partitioned columns are excluded
         --read_path_partitions            Specifies to read partitions in path specified by partition_by. By default assumes columns are in destination file.
         --format <format>                 Expected file format. Defaults to inferring from path
         --prefix <prefix>                 Optional common prefix for output files. Applied to partitioned output only.

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
- ~For the same reason API is not uniform, often functionally similar APIs would require completely different approach (see difference between reading parquet and CSV)~
    - This has been improved and now API is more or less uniform
- Regular blocking calls without need of async runtime
- Can transform underlying data if not supposed (e.g. INT96)
- Not very well documented rust API, but python bindings are easy to navigate on how to use particular Rust APIs (in addition to having high level docs that show entry points to variety of methods)
- Heavier dependency wise so slower compile times but code size is as fat as datafusion, so not a big deal
- Hard-coded to write footer into parquet, resulting in output with overhead

#### Partitions inference

By default `polars` will infer partitions for single directory inputs, otherwise user need to specify `hive_options` in `ScanArgsParquet`

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

#### Partitions inference

By default `datafusion` can recursively dwell into folder, but it will be unable to determine partitions automatically.
To enable correct partitioning you need to specify `table_partition_cols` for `read_*` method, otherwise datafusion will be unable to handle HIVE style parquet files as partition columns are not included and should be inferred from file path

**NOTE:** You must make sure to use `table_partition_cols` only if file doesn't contain this column, otherwise it will cause duplicate column error

#### Partitions filtering

By default datafusion normal API will not be able to optimize loads using known partition filters.
The only option is to manually query all files to read via [ListingTable::list_files_for_scan](https://docs.rs/datafusion-catalog-listing/54.0.0/datafusion_catalog_listing/struct.ListingTable.html#method.list_files_for_scan)

#### Caching behavior

When listing files with partition pruning datafusion has nasty bug where it performs full table scan on cache miss:
https://github.com/apache/datafusion/issues/23341

This results in significant increase of workload even if you only need to fetch specific sub-directory.
Therefore it is recommended to disable cache for file listing:
https://github.com/DoumanAsh/mishka/blob/87454fb37312dd0143823accd9cc9bd5be39230f/src/datafusion.rs#L168
