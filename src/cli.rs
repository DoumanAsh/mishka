//! Command line arguments
use arg::Args;

use crate::{ExpectFormat, Int96Timestamp};

#[derive(Copy, Clone, Debug)]
///Backend to use
pub enum Backend {
    ///Polars
    ///
    ///Has limited compatibility with deprecated formats
    Polars,
    ///Datafusion
    ///
    ///Unique selection always requires at least one `unique_by` argument
    Datafusion,
}

impl Backend {
    #[inline]
    ///Indicates polars is selected
    pub const fn is_polars(&self) -> bool {
        matches!(self, Self::Polars)
    }

    #[inline]
    ///Indicates polars is selected
    pub const fn is_datafusion(&self) -> bool {
        matches!(self, Self::Datafusion)
    }
}

impl core::str::FromStr for Backend {
    type Err = &'static str;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.eq_ignore_ascii_case("polars") {
            Ok(Self::Polars)
        } else if text.eq_ignore_ascii_case("datafusion") {
            Ok(Self::Datafusion)
        } else {
            Err("Allowed values: polars")
        }
    }
}

impl core::str::FromStr for Int96Timestamp {
    type Err = &'static str;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.eq_ignore_ascii_case("ns") || text.eq_ignore_ascii_case("nanosecond") {
            Ok(Self::Ns)
        } else if text.eq_ignore_ascii_case("us") || text.eq_ignore_ascii_case("microsecond") {
            Ok(Self::Us)
        } else if text.eq_ignore_ascii_case("ms") || text.eq_ignore_ascii_case("millisecond") {
            Ok(Self::Ms)
        } else if text.eq_ignore_ascii_case("s") || text.eq_ignore_ascii_case("second") {
            Ok(Self::S)
        } else {
            Err("Allowed values: ns, us, ms, s")
        }
    }
}


#[derive(Args, Debug)]
///Query data
pub struct Query {
    #[arg(long, default_value = "1000")]
    ///Limit number of elements to process at most. Default size 1000
    pub chunk_by: usize,
    #[arg(required)]
    ///Path(s) to a file or directory (may be URI or include wildcard)
    pub path: String,
}

#[derive(Args, Debug)]
///Concatenates queried data into single file
pub struct Concat {
    #[arg(long)]
    ///List of column names to partition by (in order)
    pub partition_by: Vec<String>,
    #[arg(long, default_value = "ExpectFormat::Infer")]
    ///Expected file format. Defaults to inferring from path
    pub format: ExpectFormat,
    #[arg(required)]
    ///Path(s) to a file or directory (may be URI or include wildcard)
    pub path: String,
    #[arg(required)]
    ///Path to a file to output (may be URI)
    pub output: String,
}

#[derive(Args, Debug)]
///Possible commands
pub enum Command {
    ///query data
    Query(Query),
    ///concat data
    Concat(Concat),
}

///Common parameters of CLI
pub struct CommonArgs {
    ///Backend to use
    pub backend: Backend,
    ///List of column names to select
    pub select: Vec<String>,
    ///List of column names to sort in order
    pub sort: Vec<String>,
    ///Specifies descending order for sort. Defaults to ascending.
    pub sort_desc: bool,
    ///Specify to select unique
    pub unique: bool,
    ///Specify columns to use to consider for uniqueness
    pub unique_by: Vec<String>,
    ///Specify to use stable operations
    pub stable: bool,
    ///Expected file format. Defaults to inferring from path
    pub format: ExpectFormat,
    ///Specifies time unit for int96. Defaults to nanosecond
    pub coerce_int96: Int96Timestamp,
}

impl CommonArgs {
    ///Creates query parameters from arguments
    pub fn into_query(
        self,
    ) -> crate::Query<
        impl ExactSizeIterator<Item = String>,
        impl ExactSizeIterator<Item = crate::SortBy>,
        impl ExactSizeIterator<Item = String>,
    > {
        crate::Query {
            column: self.select.into_iter(),
            sort_by: self.sort.into_iter().map(move |column| crate::SortBy {
                column,
                desc: self.sort_desc,
            }),
            unique: self.unique.then(move || crate::Unique {
                columns: self.unique_by.into_iter(),
                is_stable: self.stable,
            }),
            coerce_int96: self.coerce_int96,
        }
    }
}

#[derive(Args, Debug)]
///mishaka 1.0.0-beta.3
///
///Utility to work with data files
pub struct Cli {
    #[arg(long, default_value = "Backend::Polars")]
    ///Specifies backend to use. Defaults to polars
    pub backend: Backend,
    #[arg(long)]
    ///List of column names to select
    pub select: Vec<String>,
    #[arg(long)]
    ///List of column names to sort in order
    pub sort: Vec<String>,
    #[arg(long)]
    ///Specifies descending order for sort. Defaults to ascending.
    pub sort_desc: bool,
    #[arg(long)]
    ///Specify to select unique
    pub unique: bool,
    #[arg(long)]
    ///Specify columns to use to consider for uniqueness
    pub unique_by: Vec<String>,
    #[arg(long)]
    ///Specify to use stable operations
    pub stable: bool,
    #[arg(long, default_value = "ExpectFormat::Infer")]
    ///Expected file format. Defaults to inferring from path
    pub format: ExpectFormat,
    #[arg(long, default_value = "Int96Timestamp::new()")]
    ///Specifies time unit for int96. Defaults to nanosecond
    pub coerce_int96: Int96Timestamp,
    #[arg(sub)]
    ///Command to run. Possible values: query, concat
    pub command: Command,
}

impl Cli {
    #[inline(always)]
    ///Splits arguments into common and command's specifics
    pub fn split_parts(self) -> (CommonArgs, Command) {
        let Self {
            select,
            sort,
            sort_desc,
            mut unique,
            unique_by,
            stable,
            format,
            coerce_int96,
            command,
            backend
        } = self;

        unique = unique | !unique_by.is_empty();
        let common = CommonArgs {
            select,
            sort,
            sort_desc,
            unique,
            unique_by,
            stable,
            format,
            coerce_int96,
            backend
        };
        (common, command)
    }
}

#[inline(always)]
///Parses command line arguments from process environment
pub fn args() -> Cli {
    arg::parse_args()
}
