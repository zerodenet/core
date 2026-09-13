use super::ControlSocket;
use base64::Engine;
use futures_util::StreamExt;
use serde::Serialize;
use std::{collections::BTreeMap, io};
use tokio_tungstenite::tungstenite::Message;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Task {
    method: String,
    url: String,
    extra: Extra,
    stream_response: bool,
}

#[derive(Default, Serialize)]
struct Extra {
    #[serde(skip_serializing_if = "Option::is_none")]
    protocol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    referrer: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    headers: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    cookies: BTreeMap<String, String>,
}

impl Task {
    pub(super) fn websocket(url: &str, early_data: Option<&[u8]>) -> Self {
        Self {
            method: "WS".into(),
            url: url.into(),
            extra: Extra {
                protocol: early_data
                    .map(|data| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)),
                ..Extra::default()
            },
            stream_response: true,
        }
    }

    pub(super) fn http(
        method: &str,
        url: &str,
        source: &http::HeaderMap,
        stream_response: bool,
    ) -> io::Result<Self> {
        let mut extra = Extra::default();
        for (name, value) in source {
            let name = name.as_str();
            let value = value.to_str().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "non-text Browser Dialer header",
                )
            })?;
            match name {
                "referer" => extra.referrer = Some(value.into()),
                "cookie" => parse_cookies(value, &mut extra.cookies),
                _ if !forbidden_fetch_header(name) => {
                    extra.headers.insert(name.into(), value.into());
                }
                _ => {}
            }
        }
        Ok(Self {
            method: method.into(),
            url: url.into(),
            extra,
            stream_response,
        })
    }
}

fn parse_cookies(value: &str, cookies: &mut BTreeMap<String, String>) {
    for part in value.split(';') {
        if let Some((name, value)) = part.trim().split_once('=') {
            if !name.is_empty() {
                cookies.insert(name.into(), value.into());
            }
        }
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
