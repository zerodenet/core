//! The URL input Vec is paid before copy; the actual Engine subscription
//! accepts the moved filter into its existing storage book before this returns.
use super::*;
use zero_runtime::configuration::{ControlBudget, ControlBudgetPermit, ControlCharge};
pub(super) fn input(
    query: &str,
    book: &std::sync::Arc<ControlBudget>,
) -> io::Result<(ControlBudgetPermit, EventFilter)> {
    let types = parse_query_param(query, "types");
    let mut bytes = 0usize;
    let mut count = 0usize;
    if let Some(types) = types {
        for item in types.split(',') {
            count = count.checked_add(1).ok_or_else(capacity)?;
            bytes = bytes.checked_add(item.trim().len()).ok_or_else(capacity)?;
        }
    }
    bytes = bytes
        .checked_add(
            count
                .checked_mul(std::mem::size_of::<String>())
                .ok_or_else(capacity)?,
        )
        .ok_or_else(capacity)?;
    let storage = book.reserve(ControlCharge::RequestBytes(bytes))?;
    let mut filter = EventFilter::default();
    filter
        .event_types
        .try_reserve_exact(count)
        .map_err(io::Error::other)?;
    if let Some(types) = types {
        for item in types.split(',') {
            filter.event_types.push(item.trim().to_owned());
        }
    }
    Ok((storage, filter))
}
fn capacity() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "SSE input filter storage overflow",
    )
}
#[cfg(test)]
#[path = "filter/tests.rs"]
mod tests;
