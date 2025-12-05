//!Formatting

use core::fmt;
use core::sync::atomic::{self, AtomicBool, AtomicUsize};

use polars::prelude::PlanCallback;

///Formatter for schema
pub struct Schema<'a>(pub &'a polars::prelude::Schema);

impl fmt::Display for Schema<'_> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names = self.0.iter_names();
        if let Some(name) = names.next() {
            fmt.write_str(name)?;

            while let Some(name) = names.next() {
                fmt.write_str(",")?;
                fmt.write_str(&name)?;
            }
        }

        Ok(())
    }
}

///Formatter for data frame
pub struct DataFrame<'a>(pub &'a polars::prelude::DataFrame);

impl fmt::Display for DataFrame<'_> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let height = self.0.height();
        let columns = self.0.get_columns();
        for idx in 0..height {
            let mut columns = columns.iter();
            if let Some(column) = columns.next() {
                if let Ok(value) = column.get(idx) {
                    fmt.write_fmt(format_args!("{}", value))?;
                } else {
                    fmt.write_str("")?;
                }
                while let Some(column) = columns.next() {
                    fmt.write_str(",")?;
                    if let Ok(value) = column.get(idx) {
                        fmt.write_fmt(format_args!("{}", value))?;
                    } else {
                        fmt.write_str("")?;
                    }
                }
            }

            fmt.write_str("\n")?;
        }

        Ok(())
    }
}

///[batch_function] state
pub struct State {
    header_done: AtomicBool,
    row_count: AtomicUsize,
}

impl State {
    const fn new() -> Self {
        Self {
            header_done: AtomicBool::new(false),
            row_count: AtomicUsize::new(0),
        }
    }

    #[inline]
    ///Returns row count
    pub fn row_count(&self) -> usize {
        self.row_count.load(atomic::Ordering::Acquire)
    }
}

///Returns global state and formatting batch function
///
///It prints every `DataFrame` in loosely CSV format
pub fn batch_function() -> (&'static State, PlanCallback<crate::DataFrame, bool>) {
    static STATE: State = State::new();

    (&STATE, PlanCallback::new(|df: crate::DataFrame| {
        if STATE.header_done.compare_exchange(false, true, atomic::Ordering::AcqRel, atomic::Ordering::Relaxed).is_ok() {
            println!("{}", Schema(df.schema()))
        }
        STATE.row_count.fetch_add(df.height(), atomic::Ordering::AcqRel);
        print!("{}", DataFrame(&df));
        Ok(false)
    }))
}
