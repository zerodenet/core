//! Shared HTTP carrier path option normalization. No proxy protocol parsing.
#[cfg(any(feature = "ws", feature = "http_upgrade"))]
pub(crate) fn path_options(value: &str) -> (String, u32) {
    let value = if value.is_empty() {
        "/".to_owned()
    } else if value.starts_with('/') {
        value.to_owned()
    } else {
        format!("/{value}")
    };
    let Some((path, query)) = value.split_once('?') else {
        return (value, 0);
    };
    let pairs: Vec<_> = url::form_urlencoded::parse(query.as_bytes()).collect();
    let early = pairs
        .iter()
        .find(|(key, _)| key == "ed")
        .and_then(|(_, value)| value.parse::<u32>().ok())
        .unwrap_or(0);
    if !pairs.iter().any(|(key, _)| key == "ed") {
        return (value, 0);
    }
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in pairs {
        if key != "ed" {
            query.append_pair(&key, &value);
        }
    }
    let query = query.finish();
    (
        if query.is_empty() {
            path.to_owned()
        } else {
            format!("{path}?{query}")
        },
        early,
    )
}

pub(crate) fn host_matches(request: &str, expected: &str) -> bool {
    if request.contains(':') {
        request
            .parse::<http::uri::Authority>()
            .ok()
            .filter(|authority| authority.port().is_some())
            .is_some_and(|authority| {
                authority
                    .host()
                    .trim_matches(['[', ']'])
                    .eq_ignore_ascii_case(expected)
            })
    } else {
        request.eq_ignore_ascii_case(expected)
    }
}
