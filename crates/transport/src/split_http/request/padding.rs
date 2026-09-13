use super::{
    metadata::{add_cookie, cookie, header, query, set_query},
    *,
};
use http::{HeaderMap, HeaderName, HeaderValue, Uri};
mod huffman;

pub(super) fn generate(method: &str, length: u32) -> String {
    if method != "tokenish" {
        return "X".repeat(length as usize);
    }
    const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    // Batch random bytes and maintain the HPACK bit count incrementally.
    // Re-encoding the complete candidate for every adjustment makes response
    // padding quadratic and delays unrelated packet uploads on this worker.
    let initial = (length as usize * 5).div_ceil(4);
    let mut value = Vec::with_capacity(initial + 150);
    let mut rng = rand::rng();
    let mut bits = 0usize;
    while value.len() < initial {
        let mut random = [0; 256];
        rand::RngCore::fill_bytes(&mut rng, &mut random);
        for byte in random {
            // Rejection sampling preserves a uniform base62 alphabet.
            if byte >= 248 {
                continue;
            }
            let byte = ALPHABET[byte as usize % 62];
            bits += huffman::bit_len(byte);
            value.push(byte);
            if value.len() == initial {
                break;
            }
        }
    }
    for n in 0..150 {
        let actual = bits.div_ceil(8);
        if actual.abs_diff(length as usize) <= 2 {
            break;
        }
        if actual < length as usize {
            let byte = if n % 2 == 0 { b'X' } else { b'Z' };
            value.push(byte);
            bits += huffman::bit_len(byte);
        } else if value.len() > 1 {
            bits -= huffman::bit_len(value.pop().unwrap());
        } else {
            break;
        }
    }
    String::from_utf8(value).expect("base62 padding")
}
impl Profile {
    pub(super) fn pad_request<B>(&self, request: &mut Request<B>) -> io::Result<()> {
        let o = &self.options;
        let (placement, key, name, method) = if o.x_padding_obfs_mode {
            (
                o.x_padding_placement.as_str(),
                o.x_padding_key.as_str(),
                o.x_padding_header.as_str(),
                o.x_padding_method.as_str(),
            )
        } else {
            ("queryInHeader", "x_padding", "referer", "repeat-x")
        };
        let value = generate(method, sample(o.x_padding_bytes));
        match placement {
            "header" => {
                request.headers_mut().insert(
                    HeaderName::from_bytes(name.as_bytes()).map_err(io::Error::other)?,
                    value.parse().map_err(io::Error::other)?,
                );
            }
            "queryInHeader" => {
                let value = format!(
                    "https://{}{}?{}",
                    self.host,
                    self.path,
                    url::form_urlencoded::Serializer::new(String::new())
                        .append_pair(key, &value)
                        .finish()
                );
                request.headers_mut().insert(
                    HeaderName::from_bytes(name.as_bytes()).map_err(io::Error::other)?,
                    value.parse().map_err(io::Error::other)?,
                );
            }
            "query" => set_query(request.uri_mut(), key, &value)?,
            "cookie" => add_cookie(request.headers_mut(), key, &value)?,
            _ => return Err(io::Error::other("invalid xhttp padding placement")),
        }
        Ok(())
    }
    pub(super) fn pad_response(&self, headers: &mut HeaderMap) -> io::Result<()> {
        let o = &self.options;
        let (placement, key, name, method) = if o.x_padding_obfs_mode {
            (
                o.x_padding_placement.as_str(),
                o.x_padding_key.as_str(),
                o.x_padding_header.as_str(),
                o.x_padding_method.as_str(),
            )
        } else {
            ("header", "x_padding", "x-padding", "repeat-x")
        };
        if placement == "query" {
            return Ok(());
        }
        let value = generate(method, sample(o.x_padding_bytes));
        match placement {
            "header" | "queryInHeader" => {
                let value = if placement == "queryInHeader" {
                    format!(
                        "?{}",
                        url::form_urlencoded::Serializer::new(String::new())
                            .append_pair(key, &value)
                            .finish()
                    )
                } else {
                    value
                };
                headers.insert(
                    HeaderName::from_bytes(name.as_bytes()).map_err(io::Error::other)?,
                    HeaderValue::from_str(&value).map_err(io::Error::other)?,
                );
            }
            "cookie" => {
                headers.append(
                    "set-cookie",
                    format!("{key}={value}; Path=/")
                        .parse()
                        .map_err(io::Error::other)?,
                );
            }
            "query" => {}
            _ => return Err(io::Error::other("invalid xhttp padding placement")),
        }
        Ok(())
    }
    pub(in crate::split_http) fn validate_padding<B>(
        &self,
        request: &Request<B>,
    ) -> io::Result<()> {
        let o = &self.options;
        let value = if !o.x_padding_obfs_mode {
            let referer = header(request.headers(), "referer");
            if referer.is_empty() {
                query(request.uri(), "x_padding")
            } else {
                referer
                    .parse::<Uri>()
                    .map(|uri| query(&uri, "x_padding"))
                    .unwrap_or_default()
            }
        } else {
            let cookie = cookie(request.headers(), &o.x_padding_key);
            let value = header(request.headers(), &o.x_padding_header);
            if !cookie.is_empty() {
                cookie
            } else if !value.is_empty() {
                if o.x_padding_placement == "header" {
                    value.into()
                } else {
                    value
                        .parse::<Uri>()
                        .map(|uri| query(&uri, &o.x_padding_key))
                        .unwrap_or_default()
                }
            } else {
                query(request.uri(), &o.x_padding_key)
            }
        };
        let (len, tolerance) = if o.x_padding_method == "tokenish" {
            (huffman::encoded_len(value.as_bytes()), 2)
        } else {
            (value.len(), 0)
        };
        if value.is_empty()
            || len < o.x_padding_bytes.from.saturating_sub(tolerance) as usize
            || len > (o.x_padding_bytes.to + tolerance) as usize
        {
            return Err(io::Error::other("invalid xhttp padding length"));
        }
        Ok(())
    }
}
