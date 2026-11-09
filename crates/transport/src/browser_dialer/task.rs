use super::ControlSocket;
use base64::Engine;
use futures_util::StreamExt;
use serde::{ser::SerializeMap, Serialize, Serializer};
use std::io;
use tokio_tungstenite::tungstenite::Message;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Task<'a> {
    method: &'a str,
    url: &'a str,
    extra: Extra<'a>,
    stream_response: bool,
}
struct Extra<'a> {
    protocol: Option<String>,
    source: Option<&'a http::HeaderMap>,
}
impl<'a> Task<'a> {
    pub(super) fn websocket(url: &'a str, early_data: Option<&[u8]>) -> Self {
        Self {
            method: "WS",
            url,
            extra: Extra {
                protocol: early_data
                    .map(|data| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)),
                source: None,
            },
            stream_response: true,
        }
    }
    pub(super) fn http(
        method: &'a str,
        url: &'a str,
        source: &'a http::HeaderMap,
        stream_response: bool,
    ) -> io::Result<Self> {
        for value in source.values() {
            value.to_str().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "non-text Browser Dialer header",
                )
            })?;
        }
        Ok(Self {
            method,
            url,
            extra: Extra {
                protocol: None,
                source: Some(source),
            },
            stream_response,
        })
    }
    pub(super) fn encoded_len(&self) -> io::Result<usize> {
        struct Count(usize);
        impl io::Write for Count {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0 = self
                    .0
                    .checked_add(bytes.len())
                    .ok_or(io::ErrorKind::InvalidInput)?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut count = Count(0);
        serde_json::to_writer(&mut count, self).map_err(io::Error::other)?;
        Ok(count.0)
    }
    pub(super) fn encode(&self, length: usize) -> io::Result<String> {
        // Exact concrete capacity is admitted by the caller before allocation.
        // The counting pass and serializer borrow identical immutable inputs.
        struct Exact(Vec<u8>);
        impl io::Write for Exact {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if bytes.len() > self.0.capacity() - self.0.len() {
                    return Err(io::ErrorKind::InvalidInput.into());
                }
                self.0.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut output = Exact(Vec::with_capacity(length));
        serde_json::to_writer(&mut output, self).map_err(io::Error::other)?;
        String::from_utf8(output.0).map_err(io::Error::other)
    }
}
impl Serialize for Extra<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        if let Some(protocol) = &self.protocol {
            map.serialize_entry("protocol", protocol)?;
        }
        if let Some(source) = self.source {
            if let Some(value) = source.get_all("referer").iter().last() {
                map.serialize_entry("referrer", value.to_str().expect("validated same header"))?;
            }
            if source
                .keys()
                .any(|name| !forbidden_fetch_header(name.as_str()))
            {
                map.serialize_entry("headers", &Headers(source))?;
            }
            if cookies(source).next().is_some() {
                map.serialize_entry("cookies", &Cookies(source))?;
            }
        }
        map.end()
    }
}
struct Headers<'a>(&'a http::HeaderMap);
impl Serialize for Headers<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        let mut previous: Option<&str> = None;
        loop {
            // Preserve the original BTreeMap order and last-value semantics
            // without allocating a second header dictionary per assignment.
            let next = self
                .0
                .keys()
                .map(|name| name.as_str())
                .filter(|name| !forbidden_fetch_header(name))
                .filter(|name| previous.is_none_or(|old| *name > old))
                .min();
            let Some(name) = next else {
                break;
            };
            let value = self
                .0
                .get_all(name)
                .iter()
                .last()
                .expect("same actual header");
            map.serialize_entry(name, value.to_str().expect("validated same header"))?;
            previous = Some(name);
        }
        map.end()
    }
}
fn cookies(source: &http::HeaderMap) -> impl Iterator<Item = (&str, &str)> {
    source
        .get_all("cookie")
        .iter()
        .flat_map(|value| value.to_str().expect("validated same header").split(';'))
        .filter_map(|part| part.trim().split_once('='))
        .filter(|(name, _)| !name.is_empty())
}
struct Cookies<'a>(&'a http::HeaderMap);
impl Serialize for Cookies<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        let mut previous: Option<&str> = None;
        loop {
            let next = cookies(self.0)
                .map(|(name, _)| name)
                .filter(|name| previous.is_none_or(|old| *name > old))
                .min();
            let Some(name) = next else {
                break;
            };
            let value = cookies(self.0)
                .filter(|(key, _)| *key == name)
                .last()
                .unwrap()
                .1;
            map.serialize_entry(name, value)?;
            previous = Some(name);
        }
        map.end()
    }
}

fn forbidden_fetch_header(name: &str) -> bool {
    matches!(
        name,
        "accept-encoding"
            | "access-control-request-headers"
            | "access-control-request-method"
            | "connection"
            | "content-length"
            | "cookie"
            | "cookie2"
            | "date"
            | "dnt"
            | "expect"
            | "host"
            | "keep-alive"
            | "origin"
            | "referer"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "via"
    ) || name.starts_with("proxy-")
        || name.starts_with("sec-")
}

pub(crate) fn validate_url(value: &str, schemes: &[&str]) -> io::Result<()> {
    let url = url::Url::parse(value).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid Browser Dialer URL: {error}"),
        )
    })?;
    if !schemes.contains(&url.scheme())
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Browser Dialer URL has an unsupported authority or scheme",
        ));
    }
    Ok(())
}

pub(super) async fn read_ack(
    socket: &mut ControlSocket,
    deadline: tokio::time::Instant,
) -> io::Result<()> {
    loop {
        let message = tokio::time::timeout_at(deadline, socket.next())
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Browser Dialer acknowledgement timed out",
                )
            })?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "Browser Dialer channel closed before acknowledgement",
                )
            })?
            .map_err(io::Error::other)?;
        match message {
            Message::Text(value) if value == "ok" => return Ok(()),
            Message::Text(value) if value.len() <= 256 => {
                return Err(io::Error::other(format!(
                    "Browser Dialer task failed: {value}"
                )))
            }
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "Browser Dialer channel closed before acknowledgement",
                ))
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid Browser Dialer acknowledgement",
                ))
            }
        }
    }
}
