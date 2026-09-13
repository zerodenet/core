use super::*;
pub(super) const MAX_HEADER: usize = 1024 * 1024;
pub(super) fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
pub(super) fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|s| s == b"\r\n\r\n")
        .map(|p| p + 4)
}
pub(super) fn header<'a>(text: &'a str, name: &str) -> Option<&'a str> {
    text.lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.trim())
}
pub(super) fn validate_upgrade(text: &str) -> io::Result<()> {
    if !header(text, "upgrade").is_some_and(|s| s.eq_ignore_ascii_case("websocket"))
        || !header(text, "connection").is_some_and(|s| s.eq_ignore_ascii_case("upgrade"))
    {
        return Err(invalid("invalid HTTPUpgrade connection headers"));
    }
    Ok(())
}
pub(super) fn validate_response(bytes: &[u8]) -> io::Result<()> {
    let text = std::str::from_utf8(bytes).map_err(|_| invalid("invalid HTTPUpgrade response"))?;
    if text.lines().next() != Some("HTTP/1.1 101 Switching Protocols") {
        return Err(invalid(
            "HTTPUpgrade response must be 101 Switching Protocols",
        ));
    }
    validate_upgrade(text)
}
pub(super) async fn read_head<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> io::Result<(Vec<u8>, Vec<u8>)> {
    let mut head = Vec::new();
    loop {
        let mut bytes = [0; 4096];
        let n = stream.read(&mut bytes).await?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        head.extend_from_slice(&bytes[..n]);
        if let Some(end) = find_header_end(&head) {
            let extra = head.split_off(end);
            return Ok((head, extra));
        }
        if head.len() >= MAX_HEADER {
            return Err(invalid("HTTPUpgrade headers too large"));
        }
    }
}
pub(super) fn write_http_request(bytes: &mut Vec<u8>, request: &Request<()>) {
    bytes.extend_from_slice(
        format!(
            "GET {} HTTP/1.1\r\n",
            request.uri().path_and_query().map_or("/", |p| p.as_str())
        )
        .as_bytes(),
    );
    for (name, value) in request.headers() {
        bytes.extend_from_slice(name.as_str().as_bytes());
        bytes.extend_from_slice(b": ");
        bytes.extend_from_slice(value.as_bytes());
        bytes.extend_from_slice(b"\r\n");
    }
    bytes.extend_from_slice(b"\r\n");
}
