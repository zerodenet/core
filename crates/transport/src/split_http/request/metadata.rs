use super::*;
use http::{HeaderMap, HeaderName, HeaderValue, Uri};
pub(super) fn header<'a>(headers: &'a HeaderMap, key: &str) -> &'a str {
    headers.get(key).and_then(|v| v.to_str().ok()).unwrap_or("")
}
pub(super) fn cookies(headers: &HeaderMap) -> impl Iterator<Item = (&str, &str)> {
    headers
        .get_all("cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|v| v.trim().split_once('='))
        .map(|(key, value)| (key, value.trim_matches('"')))
}
pub(super) fn cookie(headers: &HeaderMap, key: &str) -> String {
    cookies(headers)
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v.to_owned())
        .unwrap_or_default()
}

pub(super) fn query(uri: &Uri, key: &str) -> String {
    url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
        .unwrap_or_default()
}
pub(super) fn set_query(uri: &mut Uri, key: &str, value: &str) -> io::Result<()> {
    let pairs: Vec<_> = url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
        .filter(|(k, _)| k != key)
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query.extend_pairs(pairs).append_pair(key, value);
    *uri = format!("{}?{}", uri.path(), query.finish())
        .parse()
        .map_err(io::Error::other)?;
    Ok(())
}
pub(super) fn add_cookie(headers: &mut HeaderMap, key: &str, value: &str) -> io::Result<()> {
    let existing = header(headers, "cookie");
    let value = format!(
        "{existing}{}{key}={value}",
        if existing.is_empty() { "" } else { "; " }
    );
    headers.insert("cookie", value.parse().map_err(io::Error::other)?);
    Ok(())
}
fn key<'a>(configured: &'a str, placement: &str, sequence: bool) -> &'a str {
    if !configured.is_empty() {
        return configured;
    }
    match (placement, sequence) {
        ("header", false) => "x-session",
        ("header", true) => "x-seq",
        (_, false) => "x_session",
        (_, true) => "x_seq",
    }
}
impl Profile {
    pub(super) fn apply_meta<B>(
        &self,
        request: &mut Request<B>,
        session: &str,
        seq: Option<u64>,
    ) -> io::Result<()> {
        let o = &self.options;
        for (placement, key, value) in [
            (
                &o.session_placement,
                key(&o.session_key, &o.session_placement, false),
                session.to_owned(),
            ),
            (
                &o.seq_placement,
                key(&o.seq_key, &o.seq_placement, true),
                seq.map(|s| s.to_string()).unwrap_or_default(),
            ),
        ] {
            if value.is_empty() {
                continue;
            }
            match placement.as_str() {
                "path" => {
                    let uri = request.uri();
                    *request.uri_mut() = format!(
                        "{}{}/{}{}",
                        uri.path().trim_end_matches('/'),
                        "",
                        value,
                        uri.query().map(|q| format!("?{q}")).unwrap_or_default()
                    )
                    .parse()
                    .map_err(io::Error::other)?;
                }
                "query" => set_query(request.uri_mut(), key, &value)?,
                "header" => {
                    request.headers_mut().insert(
                        HeaderName::from_bytes(key.as_bytes()).map_err(io::Error::other)?,
                        HeaderValue::from_str(&value).map_err(io::Error::other)?,
                    );
                }
                "cookie" => add_cookie(request.headers_mut(), key, &value)?,
                _ => return Err(io::Error::other("invalid xhttp metadata placement")),
            }
        }
        Ok(())
    }
    pub(in crate::split_http) fn meta<B>(
        &self,
        request: &Request<B>,
    ) -> io::Result<(String, Option<u64>)> {
        let suffix = request
            .uri()
            .path()
            .strip_prefix(&self.path)
            .ok_or_else(|| io::Error::other("xhttp path mismatch"))?;
        let mut parts = suffix.split('/');
        let o = &self.options;
        let mut extract = |placement: &str, key: &str| match placement {
            "path" => parts.next().unwrap_or("").to_owned(),
            "query" => query(request.uri(), key),
            "header" => header(request.headers(), key).into(),
            "cookie" => cookie(request.headers(), key),
            _ => String::new(),
        };
        let session = extract(
            &o.session_placement,
            key(&o.session_key, &o.session_placement, false),
        );
        let seq = extract(&o.seq_placement, key(&o.seq_key, &o.seq_placement, true));
        if session.len() > 64
            || !session
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-')
        {
            return Err(io::Error::other("invalid xhttp session"));
        }
        let seq = if seq.is_empty() {
            None
        } else {
            Some(seq.parse().map_err(io::Error::other)?)
        };
        Ok((session, seq))
    }
}
