use http::{Method, Request};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
fn seconds(time: SystemTime) -> SystemTime {
    UNIX_EPOCH
        + Duration::from_secs(
            time.duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        )
}
fn header<'a>(request: &'a Request<()>, name: &str) -> Option<&'a str> {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
}
fn date(request: &Request<()>, name: &str) -> Option<SystemTime> {
    header(request, name).and_then(|value| httpdate::parse_http_date(value).ok())
}
fn star(mut value: &str) -> bool {
    // There is no generated ETag for a file. Only a wildcard can match.
    loop {
        value = value.trim_start_matches([' ', '\t', ',']);
        if value.starts_with('*') {
            return true;
        }
        value = value.strip_prefix("W/").unwrap_or(value);
        let Some(rest) = value.strip_prefix('"') else {
            return false;
        };
        let Some(end) = rest.find('"') else {
            return false;
        };
        if rest[..end].bytes().any(|byte| byte < 0x21 || byte == 0x7f) {
            return false;
        }
        value = &rest[end + 1..];
    }
}
pub(super) fn evaluate(request: &Request<()>, modified: Option<SystemTime>) -> Option<u16> {
    let safe = request.method() == Method::GET || request.method() == Method::HEAD;
    if let Some(value) = header(request, "if-match") {
        if !star(value) {
            return Some(412);
        }
    } else if let (Some(since), Some(modified)) = (date(request, "if-unmodified-since"), modified) {
        if seconds(modified) > since {
            return Some(412);
        }
    }
    if let Some(value) = header(request, "if-none-match") {
        if star(value) {
            return Some(if safe { 304 } else { 412 });
        }
    } else if safe && unmodified(request, modified) {
        return Some(304);
    }
    None
}
pub(super) fn unmodified(request: &Request<()>, modified: Option<SystemTime>) -> bool {
    matches!(request.method(), &Method::GET | &Method::HEAD)
        && date(request, "if-modified-since")
            .zip(modified)
            .is_some_and(|(since, modified)| seconds(modified) <= since)
}
pub(super) fn range(request: &Request<()>, modified: Option<SystemTime>) -> Option<&str> {
    if matches!(request.method(), &Method::GET | &Method::HEAD)
        && header(request, "if-range").is_some()
        && !date(request, "if-range")
            .zip(modified)
            .is_some_and(|(since, modified)| since == seconds(modified))
    {
        return None;
    }
    header(request, "range")
}
