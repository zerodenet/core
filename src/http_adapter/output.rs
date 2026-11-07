//! Measure the same owned response before allocating its sole wire buffer.
use super::*;
use zero_runtime::configuration::{ControlBudget, ControlCharge};
const AFTER_LENGTH:&str="\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nAccess-Control-Allow-Headers: Authorization, Content-Type, X-Zero-Api-Key\r\nConnection: close\r\n\r\n";
fn headers(status: &str, length: usize, out: &mut impl std::fmt::Write) -> std::fmt::Result {
    write!(
        out,
        "{status}Content-Type: application/json\r\nContent-Length: {length}{AFTER_LENGTH}"
    )
}
struct Count(usize);
impl std::fmt::Write for Count {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.0 = self.0.checked_add(value.len()).ok_or(std::fmt::Error)?;
        Ok(())
    }
}
pub(super) async fn write_response(
    stream: &mut (impl tokio::io::AsyncWrite + Unpin),
    status: &str,
    body: &body::HttpBody,
    budget: &Arc<ControlBudget>,
) -> io::Result<()> {
    let length = body.len()?;
    let mut count = Count(0);
    headers(status, length, &mut count).map_err(io::Error::other)?;
    let total = length
        .checked_add(count.0)
        .ok_or_else(|| io::Error::other("HTTP response length overflow"))?;
    if total > budget.limits().messages.response_bytes {
        return Err(input::invalid(
            "HTTP response envelope exceeds configured limit",
        ));
    }
    let _storage = budget
        .reserve(ControlCharge::ResponseBytes(total))
        .map_err(input::capacity)?;
    let mut head = String::new();
    head.try_reserve_exact(count.0).map_err(io::Error::other)?;
    headers(status, length, &mut head).map_err(io::Error::other)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(io::Error::other)?;
    body.write_json(&mut bytes)?;
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&bytes).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests;
