//! Common utilities

use std::time;

///Returns current unix time
pub fn unit_now() -> time::Duration {
    time::SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap_or(time::Duration::ZERO)
}
