use super::super::*;
pub(super) async fn serve_using(
    path: PathBuf,
    head: bool,
    exchange: &mut dyn HttpExchange,
    modified: Option<std::time::SystemTime>,
    files: &mut super::Access<'_>,
) -> io::Result<()> {
    let mut names = match files.list(path).await {
        Ok(names) => names,
        Err(error) if error.kind() == io::ErrorKind::OutOfMemory => {
            return super::super::error(exchange, 403, head).await
        }
        Err(error) => return Err(error),
    };
    names.sort();
    let mut body = String::from(
        "<!doctype html>\n<meta name=\"viewport\" content=\"width=device-width\">\n<pre>\n",
    );
    for name in names {
        let escaped = name
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&#34;")
            .replace('\'', "&#39;");
        let target = relative_url(&name);
        body.push_str(&format!("<a href=\"{target}\">{escaped}</a>\n"));
    }
    body.push_str("</pre>\n");
    let mut response = http::Response::builder().header("content-type", "text/html; charset=utf-8");
    if let Some(modified) = modified {
        response = response.header("last-modified", httpdate::fmt_http_date(modified));
    }
    respond(
        exchange,
        response.body(()).unwrap(),
        Bytes::from(body),
        head,
    )
    .await
}

// net/url.URL{Path: name}.String(): preserve path delimiters and prevent a
// colon in a relative first segment from being interpreted as a URL scheme.
fn relative_url(name: &str) -> String {
    use std::fmt::Write;
    let mut url = String::new();
    if name
        .split('/')
        .next()
        .is_some_and(|part| part.contains(':'))
    {
        url.push_str("./");
    }
    for byte in name.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~$&+,/:;=@".contains(&byte) {
            url.push(char::from(byte));
        } else {
            write!(&mut url, "%{byte:02X}").unwrap();
        }
    }
    url
}
