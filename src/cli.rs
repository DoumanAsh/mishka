use arg::Args;

use core::fmt;
use core::str::FromStr;

#[derive(Debug)]
pub enum Format {
    Infer,
    Csv,
    Parquet,
}

impl FromStr for Format {
    type Err = &'static str;
    #[inline(always)]
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.eq_ignore_ascii_case("csv") {
            Ok(Self::Csv)
        } else if text.eq_ignore_ascii_case("parquet") {
            Ok(Self::Parquet)
        } else {
            Err("Invalid format. Allowed: 'csv' or 'parquet'")
        }
    }
}


#[repr(transparent)]
pub struct Str(pub mishka::PlSmallStr);

impl fmt::Debug for Str {
    #[inline(always)]
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, fmt)
    }
}

impl FromStr for Str {
    type Err = core::convert::Infallible;
    #[inline(always)]
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Ok(Str(mishka::PlSmallStr::from_str(text)))
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
    pub path: Str,
}

#[derive(Args, Debug)]
///Possible commands
pub enum Command {
    ///query data
    Query(Query),
}

pub struct CommonArgs {
    pub select: Vec<Str>,
    pub sort: Vec<Str>,
    pub sort_desc: bool,
    pub unique: bool,
    pub unique_by: Vec<Str>,
    pub stable: bool,
    pub format: Format,
}

impl CommonArgs {
    pub fn into_query(self) -> mishka::Query<impl ExactSizeIterator<Item = mishka::PlSmallStr>, impl ExactSizeIterator<Item = mishka::SortBy>, impl ExactSizeIterator<Item = mishka::PlSmallStr>> {

        mishka::Query {
            column: self.select.into_iter().map(move |column| column.0),
            sort_by: self.sort.into_iter().map(move |column| mishka::SortBy {
                column: column.0, desc: self.sort_desc
            }),
            unique: self.unique.then(move || mishka::Unique {
                columns: self.unique_by.into_iter().map(|column| column.0),
                is_stable: self.stable
            })
        }
    }
}

#[derive(Args, Debug)]
///mishaka 1.0.0-beta.1
///
///Utility to work with data files
pub struct Cli {
    #[arg(long)]
    ///List of column names to select
    pub select: Vec<Str>,
    #[arg(long)]
    ///List of column names to sort in order
    pub sort: Vec<Str>,
    #[arg(long)]
    ///Specifies descending order for sort. Defaults to ascending.
    pub sort_desc: bool,
    #[arg(long)]
    ///Specify to select unique
    pub unique: bool,
    #[arg(long)]
    ///Specify columns to use to consider for uniqueness
    pub unique_by: Vec<Str>,
    #[arg(long)]
    ///Specify to use stable operations
    pub stable: bool,
    #[arg(long, default_value = "Format::Infer")]
    ///Expected file format. Defaults to inferring from path
    pub format: Format,
    #[arg(sub)]
    ///Command to run. Possible values: query
    pub command: Command
}

impl Cli {
    #[inline(always)]
    pub fn split_parts(self) -> (CommonArgs, Command) {
        let Self { select, sort, sort_desc, mut unique, unique_by, stable, format, command } = self;

        unique = unique | !unique_by.is_empty();
        let common = CommonArgs {
            select,
            sort,
            sort_desc,
            unique,
            unique_by,
            stable,
            format,
        };
        (common, command)
    }
}

#[inline(always)]
pub fn args() -> Cli {
    arg::parse_args()
}
