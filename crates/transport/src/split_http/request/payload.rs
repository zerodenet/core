use super::{
    metadata::{add_cookie, cookies, header},
    *,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use bytes::Bytes;
impl Profile {
    pub(in crate::split_http) fn transmit_peak(&self, length: usize) -> io::Result<usize> {
        if !matches!(self.mode, XhttpMode::PacketUp | XhttpMode::Auto)
            || !matches!(
                self.options.uplink_data_placement.as_str(),
                "header" | "cookie"
            )
        {
            return Ok(length);
        }
        // Base64 String and retained headers/cookies coexist with old/new cookie
        // concatenation. All derived business-byte copies precede actual send.
        let encoded = length
            .checked_add(2)
            .and_then(|v| v.checked_div(3))
            .and_then(|v| v.checked_mul(4))
            .ok_or(io::ErrorKind::InvalidInput)?;
        let chunks = encoded.div_ceil(self.options.uplink_chunk_size.from.max(64) as usize);
        let keys = self
            .options
            .uplink_data_key
            .len()
            .checked_add(24)
            .and_then(|v| v.checked_mul(chunks))
            .ok_or(io::ErrorKind::InvalidInput)?;
        encoded
            .checked_mul(4)
            .and_then(|v| keys.checked_mul(3).and_then(|keys| v.checked_add(keys)))
            .and_then(|v| v.checked_add(length))
            .ok_or(io::ErrorKind::InvalidInput.into())
    }
    pub(in crate::split_http) fn packet_request(
        &self,
        session: &str,
        seq: u64,
        payload: &[u8],
    ) -> io::Result<Request<Body>> {
        self.packet_request_owned(session, seq, Bytes::copy_from_slice(payload))
    }
    pub(in crate::split_http) fn packet_request_owned(
        &self,
        session: &str,
        seq: u64,
        payload: Bytes,
    ) -> io::Result<Request<Body>> {
        let o = &self.options;
        let embedded = matches!(o.uplink_data_placement.as_str(), "header" | "cookie");
        let body = if embedded {
            Body::empty()
        } else {
            Body::full(payload.clone())
        };
        let mut request = self.request(&o.uplink_http_method, session, Some(seq), body)?;
        if !embedded {
            request.headers_mut().insert(
                "content-length",
                payload
                    .len()
                    .to_string()
                    .parse()
                    .map_err(io::Error::other)?,
            );
        }
        if embedded {
            let encoded = URL_SAFE_NO_PAD.encode(&payload);
            let mut remaining = encoded.as_str();
            let mut i = 0;
            while !remaining.is_empty() {
                let size = (sample(o.uplink_chunk_size) as usize).min(remaining.len());
                let (chunk, tail) = remaining.split_at(size);
                remaining = tail;
                if o.uplink_data_placement == "header" {
                    request.headers_mut().insert(
                        http::HeaderName::from_bytes(
                            format!("{}-{i}", o.uplink_data_key).as_bytes(),
                        )
                        .map_err(io::Error::other)?,
                        chunk.parse().map_err(io::Error::other)?,
                    );
                } else {
                    add_cookie(
                        request.headers_mut(),
                        &format!("{}_{i}", o.uplink_data_key),
                        chunk,
                    )?;
                }
                i += 1;
            }
        }
        Ok(request)
    }
    /// Auto concatenates header, cookie, then body, matching the reference.
    pub(in crate::split_http) fn packet_prefix<B>(
        &self,
        request: &Request<B>,
    ) -> io::Result<Vec<u8>> {
        let o = &self.options;
        let mut payload = Vec::new();
        for placement in ["header", "cookie"] {
            if o.uplink_data_placement != "auto" && o.uplink_data_placement != placement {
                continue;
            }
            // Validate and split each Cookie header once. Looking up every
            // numbered chunk by reparsing the full header is quadratic and can
            // delay earlier uploads behind unrelated HTTP connections.
            let mut cookie_values = std::collections::BTreeMap::new();
            if placement == "cookie" {
                for (key, value) in cookies(request.headers()) {
                    cookie_values.entry(key).or_insert(value);
                }
            }
            let mut encoded = String::new();
            for i in 0.. {
                let chunk = if placement == "header" {
                    header(request.headers(), &format!("{}-{i}", o.uplink_data_key))
                } else {
                    cookie_values
                        .get(format!("{}_{i}", o.uplink_data_key).as_str())
                        .copied()
                        .unwrap_or("")
                };
                if chunk.is_empty() {
                    break;
                }
                if encoded.len() + chunk.len()
                    > (o.sc_max_each_post_bytes.to as usize).div_ceil(3) * 4
                {
                    return Err(io::Error::new(
                        io::ErrorKind::FileTooLarge,
                        "xhttp encoded payload too large",
                    ));
                }
                encoded.push_str(chunk);
            }
            payload.extend_from_slice(&URL_SAFE_NO_PAD.decode(encoded).map_err(io::Error::other)?);
            if payload.len() > o.sc_max_each_post_bytes.to as usize {
                return Err(io::Error::new(
                    io::ErrorKind::FileTooLarge,
                    "xhttp payload too large",
                ));
            }
        }
        Ok(payload)
    }
}
