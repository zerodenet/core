//! Built-in extension types from Go 1.26.1 mime/type.go.
//! Copyright The Go Authors; see ../LICENSE-Go for the BSD license.

pub(super) fn builtin(extension: &str) -> Option<&'static str> {
    match extension.to_ascii_lowercase().as_str() {
        "ai" => Some("application/postscript"),
        "apk" => Some("application/vnd.android.package-archive"),
        "apng" => Some("image/apng"),
        "avif" => Some("image/avif"),
        "bin" => Some("application/octet-stream"),
        "bmp" => Some("image/bmp"),
        "com" => Some("application/octet-stream"),
        "css" => Some("text/css; charset=utf-8"),
        "csv" => Some("text/csv; charset=utf-8"),
        "doc" => Some("application/msword"),
        "docx" => Some("application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        "ehtml" => Some("text/html; charset=utf-8"),
        "eml" => Some("message/rfc822"),
        "eps" => Some("application/postscript"),
        "exe" => Some("application/octet-stream"),
        "flac" => Some("audio/flac"),
        "gif" => Some("image/gif"),
        "gz" => Some("application/gzip"),
        "htm" => Some("text/html; charset=utf-8"),
        "html" => Some("text/html; charset=utf-8"),
        "ico" => Some("image/vnd.microsoft.icon"),
        "ics" => Some("text/calendar; charset=utf-8"),
        "jfif" => Some("image/jpeg"),
        "jpeg" => Some("image/jpeg"),
        "jpg" => Some("image/jpeg"),
        "js" => Some("text/javascript; charset=utf-8"),
        "json" => Some("application/json"),
        "m4a" => Some("audio/mp4"),
        "mjs" => Some("text/javascript; charset=utf-8"),
        "mp3" => Some("audio/mpeg"),
        "mp4" => Some("video/mp4"),
        "oga" => Some("audio/ogg"),
        "ogg" => Some("audio/ogg"),
        "ogv" => Some("video/ogg"),
        "opus" => Some("audio/ogg"),
        "pdf" => Some("application/pdf"),
        "pjp" => Some("image/jpeg"),
        "pjpeg" => Some("image/jpeg"),
        "png" => Some("image/png"),
        "ppt" => Some("application/vnd.ms-powerpoint"),
        "pptx" => Some("application/vnd.openxmlformats-officedocument.presentationml.presentation"),
        "ps" => Some("application/postscript"),
        "rdf" => Some("application/rdf+xml"),
        "rtf" => Some("application/rtf"),
        "shtml" => Some("text/html; charset=utf-8"),
        "svg" => Some("image/svg+xml"),
        "text" => Some("text/plain; charset=utf-8"),
        "tif" => Some("image/tiff"),
        "tiff" => Some("image/tiff"),
        "txt" => Some("text/plain; charset=utf-8"),
        "vtt" => Some("text/vtt; charset=utf-8"),
        "wasm" => Some("application/wasm"),
        "wav" => Some("audio/wav"),
        "webm" => Some("audio/webm"),
        "webp" => Some("image/webp"),
        "xbl" => Some("text/xml; charset=utf-8"),
        "xbm" => Some("image/x-xbitmap"),
        "xht" => Some("application/xhtml+xml"),
        "xhtml" => Some("application/xhtml+xml"),
        "xls" => Some("application/vnd.ms-excel"),
        "xlsx" => Some("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        "xml" => Some("text/xml; charset=utf-8"),
        "xsl" => Some("text/xml; charset=utf-8"),
        "zip" => Some("application/zip"),
        _ => None,
    }
}

#[path = "mime/system.rs"]
mod system;

pub(in crate::hysteria::masquerade::file) fn initialize() {
    system::database();
}

pub(super) fn lookup(extension: &str) -> Option<&'static str> {
    system::database().lookup(extension)
}

const BUILTIN_EXTENSIONS: &[&str] = &[
    "ai", "apk", "apng", "avif", "bin", "bmp", "com", "css", "csv", "doc", "docx", "ehtml", "eml",
    "eps", "exe", "flac", "gif", "gz", "htm", "html", "ico", "ics", "jfif", "jpeg", "jpg", "js",
    "json", "m4a", "mjs", "mp3", "mp4", "oga", "ogg", "ogv", "opus", "pdf", "pjp", "pjpeg", "png",
    "ppt", "pptx", "ps", "rdf", "rtf", "shtml", "svg", "text", "tif", "tiff", "txt", "vtt", "wasm",
    "wav", "webm", "webp", "xbl", "xbm", "xht", "xhtml", "xls", "xlsx", "xml", "xsl", "zip",
];
