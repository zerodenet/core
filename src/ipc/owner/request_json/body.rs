//! Actual API collections only. The probe is paid before Value construction,
//! then freed before the original decoder runs on the same immutable wire.
use super::*;
pub(super) fn bytes(raw: &[u8], book: &Arc<ControlBudget>, query: bool) -> io::Result<usize> {
    let value_bytes = estimate::value(raw, book)?;
    let _probe = book.reserve(ControlCharge::RequestBytes(value_bytes))?;
    let extra = match serde_json::from_slice::<Value>(raw) {
        Ok(value) => {
            if query {
                owned::query(&value)?
            } else {
                match (
                    value.get("method").and_then(Value::as_str),
                    value.get("params"),
                ) {
                    (Some(method), Some(params)) => owned::command(method, params, book)?,
                    _ => 0,
                }
            }
        }
        // A Command may legally ignore a magic field which Value interprets.
        // Preserve its actual decoder; only this unusual probe failure needs
        // the largest actual API row as a conservative capacity bound.
        Err(_) if !query => estimate::rows(
            raw,
            book,
            std::mem::size_of::<zero_api::StatsResetTarget>().max(std::mem::size_of::<String>()),
        )?,
        Err(_) => 0,
    };
    sum(
        sum(value_bytes, extra)?,
        if query {
            0
        } else {
            estimate::content(raw, book)?
        },
    )
}
/// Held by this original HTTP request through accepted work/real joins.
pub(crate) fn http(
    method: &str,
    path: &str,
    raw: &[u8],
    book: &Arc<ControlBudget>,
) -> io::Result<ControlBudgetPermit> {
    let path = path.split('?').next().unwrap_or(path);
    let json = method == "POST" && matches!(path, "/api/v1/query" | "/api/v1/commands");
    if !json {
        return book.reserve(ControlCharge::RequestBytes(raw.len()));
    }
    let work = workspace(raw, book)?;
    let bytes = sum(
        bytes(raw, book, path == "/api/v1/query")?,
        sum(sum(raw.len(), 4096)?, estimate::scratch(raw)?)?,
    )?;
    drop(work); // all measurement data/errors returned before final ownership
    let held = book.reserve(ControlCharge::RequestBytes(bytes))?;
    Ok(held)
}
