//!Datafusion formatting
use super::{Schema, DataFrame};

use core::fmt;

use datafusion::common::arrow::array::RecordBatch;
use datafusion::common::arrow::datatypes::Schema as DatafusionSchema;
use datafusion::error::DataFusionError;
use futures_util::StreamExt;

impl fmt::Display for Schema<'_, DatafusionSchema> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut fields = self.0.fields.iter();
        if let Some(field) = fields.next() {
            fmt.write_str(field.name())?;

            while let Some(field) = fields.next() {
                fmt.write_str(",")?;
                fmt.write_str(field.name())?;
            }
        }

        Ok(())
    }
}

impl fmt::Display for DataFrame<'_, RecordBatch> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use datafusion::common::arrow::util::display::{ArrayFormatter, FormatOptions};

        let options = FormatOptions::new().with_display_error(true);
        let height = self.0.num_rows();
        let columns = self.0.columns();

        for idx in 0..height {
            let mut columns = columns.iter();

            if let Some(column) = columns.next() {
                match ArrayFormatter::try_new(column.as_ref(), &options) {
                    Ok(formatter) => fmt.write_fmt(format_args!("{}", formatter.value(idx)))?,
                    Err(_) => fmt.write_str("")?,
                };
                while let Some(column) = columns.next() {
                    fmt.write_str(",")?;

                    match ArrayFormatter::try_new(column.as_ref(), &options) {
                        Ok(formatter) => fmt.write_fmt(format_args!("{}", formatter.value(idx)))?,
                        Err(_) => fmt.write_str("")?
                    };
                }
            }

            fmt.write_str("\n")?;

        }

        Ok(())
    }
}

///Prints partitioned data returning number of rows
pub async fn format_partitioned_data(stream: Vec<datafusion::execution::SendableRecordBatchStream>) -> Result<usize, DataFusionError> {
    let mut count = 0usize;
    let mut is_first = true;

    for mut partition in stream {
        if is_first {
            is_first = false;
            let schema = partition.schema();
            println!("{}", Schema(&*schema));
        }

        while let Some(record) = partition.next().await {
            let record = record?;
            count = count.saturating_add(record.num_rows());
            print!("{}", DataFrame(&record));
        }
    }

    Ok(count)
}
