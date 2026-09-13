//! Content detection compatible with net/http in Go 1.26.1.
//! Signature data adapted from Go's net/http/sniff.go (Go Authors, 2011).
//! See LICENSE-Go in this directory for the BSD license.

const TEXT: &str = "text/plain; charset=utf-8";

pub(super) fn sniff(body: &[u8]) -> &'static str {
    let sample = &body[..body.len().min(512)];
    let trim = sample.trim_ascii_start();
    for tag in [
        b"<!DOCTYPE HTML".as_slice(),
        b"<HTML",
        b"<HEAD",
        b"<SCRIPT",
        b"<IFRAME",
        b"<H1",
        b"<DIV",
        b"<FONT",
        b"<TABLE",
        b"<A",
        b"<STYLE",
        b"<TITLE",
        b"<B",
        b"<BODY",
        b"<BR",
        b"<P",
        b"<!--",
    ] {
        if trim
            .get(..tag.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(tag))
            && matches!(trim.get(tag.len()), Some(b' ' | b'>'))
        {
            return "text/html; charset=utf-8";
        }
    }
    if trim.starts_with(b"<?xml") {
        return "text/xml; charset=utf-8";
    }
    if sample.starts_with(b"%PDF-") {
        return "application/pdf";
    }
    if sample.starts_with(b"%!PS-Adobe-") {
        return "application/postscript";
    }
    if sample.len() >= 4 {
        if sample.starts_with(b"\xfe\xff") {
            return "text/plain; charset=utf-16be";
        }
        if sample.starts_with(b"\xff\xfe") {
            return "text/plain; charset=utf-16le";
        }
        if sample.starts_with(b"\xef\xbb\xbf") {
            return TEXT;
        }
    }
    if let Some(mime) = exact(
        sample,
        &[
            (b"\0\0\x01\0", "image/x-icon"),
            (b"\0\0\x02\0", "image/x-icon"),
            (b"BM", "image/bmp"),
            (b"GIF87a", "image/gif"),
            (b"GIF89a", "image/gif"),
        ],
    ) {
        return mime;
    }
    if masked(sample, b"RIFF", b"WEBPVP") {
        return "image/webp";
    }
    if let Some(mime) = exact(
        sample,
        &[
            (b"\x89PNG\r\n\x1a\n", "image/png"),
            (b"\xff\xd8\xff", "image/jpeg"),
        ],
    ) {
        return mime;
    }
    if masked(sample, b"FORM", b"AIFF") {
        return "audio/aiff";
    }
    if let Some(mime) = exact(
        sample,
        &[
            (b"ID3", "audio/mpeg"),
            (b"OggS\0", "application/ogg"),
            (b"MThd\0\0\0\x06", "audio/midi"),
        ],
    ) {
        return mime;
    }
    if masked(sample, b"RIFF", b"AVI ") {
        return "video/avi";
    }
    if masked(sample, b"RIFF", b"WAVE") {
        return "audio/wave";
    }
    if mp4(sample) {
        return "video/mp4";
    }
    if sample.starts_with(b"\x1a\x45\xdf\xa3") {
        return "video/webm";
    }
    if sample.get(34..36) == Some(b"LP") {
        return "application/vnd.ms-fontobject";
    }
    if let Some(mime) = exact(
        sample,
        &[
            (b"\0\x01\0\0", "font/ttf"),
            (b"OTTO", "font/otf"),
            (b"ttcf", "font/collection"),
            (b"wOFF", "font/woff"),
            (b"wOF2", "font/woff2"),
            (b"\x1f\x8b\x08", "application/x-gzip"),
            (b"PK\x03\x04", "application/zip"),
            (b"Rar!\x1a\x07\0", "application/x-rar-compressed"),
            (b"Rar!\x1a\x07\x01\0", "application/x-rar-compressed"),
            (b"\0asm", "application/wasm"),
        ],
    ) {
        return mime;
    }
    if sample
        .iter()
        .any(|b| matches!(*b, 0..=8 | 11 | 14..=26 | 28..=31))
    {
        "application/octet-stream"
    } else {
        TEXT
    }
}

fn exact(sample: &[u8], signatures: &[(&[u8], &'static str)]) -> Option<&'static str> {
    signatures
        .iter()
        .find_map(|(prefix, mime)| sample.starts_with(prefix).then_some(*mime))
}
fn masked(sample: &[u8], prefix: &[u8; 4], suffix: &[u8]) -> bool {
    sample.starts_with(prefix) && sample.get(8..8 + suffix.len()) == Some(suffix)
}
fn mp4(sample: &[u8]) -> bool {
    if sample.len() < 12 || &sample[4..8] != b"ftyp" {
        return false;
    }
    let length = u32::from_be_bytes(sample[..4].try_into().unwrap()) as usize;
    length <= sample.len()
        && length.is_multiple_of(4)
        && (8..length)
            .step_by(4)
            .any(|offset| offset != 12 && &sample[offset..offset + 3] == b"mp4")
}
