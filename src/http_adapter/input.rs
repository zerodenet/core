//! The one original HTTP request keeps its actual buffers and decoded payload credit.
use super::*;
use zero_runtime::configuration::{ControlBudget, ControlBudgetPermit, ControlCharge};
#[derive(Debug)]
enum ClientFrame {
    Invalid(&'static str),
    Capacity(io::Error),
}
impl std::fmt::Display for ClientFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) => f.write_str(message),
            Self::Capacity(error) => write!(f, "HTTP control frame capacity exhausted: {error}"),
        }
    }
}
impl std::error::Error for ClientFrame {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Invalid(_) => None,
            Self::Capacity(error) => Some(error),
        }
    }
}
pub(super) fn client_frame(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.is::<ClientFrame>())
}
pub(super) fn capacity(error: io::Error) -> io::Error {
    if error.kind() == io::ErrorKind::WouldBlock {
        io::Error::new(io::ErrorKind::InvalidData, ClientFrame::Capacity(error))
    } else {
        error
    }
}
pub(super) struct HeldRequest {
    request: HttpRequest,
    _payload: Vec<ControlBudgetPermit>,
}
impl std::ops::Deref for HeldRequest {
    type Target = HttpRequest;
    fn deref(&self) -> &Self::Target {
        &self.request
    }
}
pub(super) async fn read_request(
    stream: &mut (impl tokio::io::AsyncRead + Unpin),
    budget: &Arc<ControlBudget>,
) -> io::Result<HeldRequest> {
    let mut head = Vec::new();
    let mut raw = budget
        .reserve(ControlCharge::RequestBytes(0))
        .map_err(capacity)?;
    loop {
        let maximum = budget.limits().messages.http_header_bytes;
        if head.len() >= maximum {
            return Err(invalid("http request head too large"));
        }
        let mut byte = [0; 1];
        if stream.read(&mut byte).await? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF while reading http request",
            ));
        }
        if head.len() == head.capacity() {
            let capacity = head
                .len()
                .checked_add(1024)
                .ok_or_else(|| invalid("http header length overflow"))?
                .min(maximum);
            raw.grow(capacity - head.capacity())
                .map_err(self::capacity)?;
            head.try_reserve_exact(capacity - head.len())
                .map_err(io::Error::other)?;
        }
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let head = String::from_utf8(head).map_err(|_| invalid("request is not utf-8"))?;
    // Method/path/header strings coexist with this original header during parse.
    let decoded = budget
        .reserve(ControlCharge::RequestBytes(head.len()))
        .map_err(capacity)?;
    let first = head
        .lines()
        .next()
        .ok_or_else(|| invalid("missing request line"))?;
    let mut parts = first.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| invalid("missing method"))?
        .to_owned();
    let path = parts
        .next()
        .ok_or_else(|| invalid("missing path"))?
        .to_owned();
    let headers = head
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_owned(), value.trim().to_owned()))
        .collect::<Vec<_>>();
    let length = content_length(&headers)?;
    if length > budget.limits().messages.http_body_bytes {
        return Err(invalid("request body too large"));
    }
    let body_charge = budget
        .reserve(ControlCharge::RequestBytes(length))
        .map_err(capacity)?;
    // The typed JSON request is admitted before the route decodes a second envelope.
    let body_decoded = budget
        .reserve(ControlCharge::RequestBytes(length))
        .map_err(capacity)?;
    let mut body = Vec::new();
    body.try_reserve_exact(length).map_err(io::Error::other)?;
    body.resize(length, 0);
    stream.read_exact(&mut body).await?;
    drop(head);
    drop(raw);
    Ok(HeldRequest {
        request: HttpRequest {
            method,
            path,
            headers,
            body,
        },
        _payload: vec![decoded, body_charge, body_decoded],
    })
}
pub(super) fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, ClientFrame::Invalid(message))
}
fn content_length(headers: &[(String, String)]) -> io::Result<usize> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map_or(Ok(0), |(_, value)| {
            value.parse().map_err(|_| invalid("invalid content-length"))
        })
}

#[cfg(test)]
mod tests;
