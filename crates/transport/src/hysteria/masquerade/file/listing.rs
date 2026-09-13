use super::super::*;
pub(super) async fn serve(
    path: PathBuf,
    head: bool,
    exchange: &mut dyn HttpExchange,
    modified: Option<std::time::SystemTime>,
) -> io::Result<()> {
    let mut entries = tokio::fs::read_dir(path).await?;
    let mut names = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let mut name = entry.file_name().to_string_lossy().into_owned();
        if entry.file_type().await?.is_dir() {
            name.push('/');
        }
        names.push(name);
        if names.len() > 65536 {
            return error(exchange, 403, head).await;
        }
    }
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
