//! Browser header values selected by the pinned carrier reference.
use crate::gorand;
use http::{HeaderMap, HeaderValue};
const FIREFOX: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0";
fn chrome_version(seed: i64, now: u64) -> u32 {
    // 2026-01-13 00:00:00 UTC, Chrome 144's anchor in Xray v26.3.27.
    let mut release = 1768262400u64;
    let mut version = 144;
    let mut rng = gorand::GoRandom::new(seed);
    while release < now {
        release += (rng.below(21) as u64 + 25) * 86400;
        version += 1;
    }
    version - 1
}
fn ch_ua(version: u32, edge: bool) -> String {
    const SYMBOLS: [&str; 11] = [" ", "(", ":", "-", ".", "/", ")", ";", "=", "?", "_"];
    const VERSIONS: [&str; 3] = ["8", "99", "24"];
    const ORDERS: [[usize; 3]; 6] = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    let base = [
        format!(
            "\"Not{}A{}Brand\";v=\"{}\"",
            SYMBOLS[version as usize % 11],
            SYMBOLS[(version as usize + 1) % 11],
            VERSIONS[version as usize % 3]
        ),
        format!("\"Chromium\";v=\"{version}\""),
        format!(
            "\"{}\";v=\"{version}\"",
            if edge {
                "Microsoft Edge"
            } else {
                "Google Chrome"
            }
        ),
    ];
    let mut ordered = [String::new(), String::new(), String::new()];
    for (i, index) in ORDERS[version as usize % 6].iter().enumerate() {
        ordered[*index] = base[i].clone();
    }
    ordered.join(", ")
}
fn set(headers: &mut HeaderMap, name: &'static str, value: impl AsRef<str>) {
    headers.insert(
        name,
        HeaderValue::from_str(value.as_ref()).expect("generated browser header"),
    );
}
fn default(headers: &mut HeaderMap, name: &'static str, value: &str) {
    if headers.get(name).is_none_or(|v| v.is_empty()) {
        set(headers, name, value);
    }
}
fn transport_headers(headers: &mut HeaderMap, version: u32, websocket: bool) {
    let browser = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("chrome")
        .to_owned();
    if browser == "golang" {
        set(headers, "user-agent", "Go-http-client/1.1");
        return;
    }
    match browser.as_str() {
        "chrome" | "edge" => {
            let edge = browser == "edge";
            let mut ua = format!("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version}.0.0.0 Safari/537.36");
            if edge {
                ua.push_str(&format!("Edg/{version}.0.0.0"));
            }
            set(headers, "user-agent", ua);
            set(headers, "sec-ch-ua", ch_ua(version, edge));
            set(headers, "sec-ch-ua-mobile", "?0");
            set(headers, "sec-ch-ua-platform", "\"Windows\"");
            set(headers, "accept-language", "en-US,en;q=0.9");
            if !websocket {
                default(headers, "priority", "u=1, i");
            }
        }
        "firefox" => {
            set(headers, "user-agent", FIREFOX);
            set(headers, "accept-language", "en-US,en;q=0.5");
            if !websocket {
                default(headers, "priority", "u=4");
            }
        }
        _ => return,
    }
    set(headers, "dnt", "1");
    set(
        headers,
        "sec-fetch-mode",
        if websocket { "websocket" } else { "cors" },
    );
    set(headers, "sec-fetch-dest", "empty");
    set(headers, "sec-fetch-site", "same-origin");
    default(headers, "cache-control", "no-cache");
    default(headers, "pragma", "no-cache");
    default(headers, "accept", "*/*");
}

fn anchored_version() -> u32 {
    static VERSION: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *VERSION.get_or_init(|| {
        let cpu = zero_platform_tokio::cpu_topology();
        let seed = cpu.family as i64
            + cpu.model as i64
            + cpu.physical_cores as i64
            + cpu.logical_cores as i64
            + cpu.cache_line as i64;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        chrome_version(seed, now)
    })
}
#[cfg(feature = "http_client")]
pub(crate) fn apply_navigation_headers(headers: &mut HeaderMap) {
    transport_headers(headers, anchored_version(), false);
    headers.remove("pragma");
    set(headers, "cache-control", "max-age=0");
    set(headers, "upgrade-insecure-requests", "1");
    set(headers, "accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/jxl,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7");
    set(headers, "sec-fetch-site", "none");
    set(headers, "sec-fetch-mode", "navigate");
    set(headers, "sec-fetch-user", "?1");
    set(headers, "sec-fetch-dest", "document");
    set(headers, "priority", "u=0, i");
}
#[cfg(feature = "split_http")]
pub(crate) fn apply_fetch_headers(headers: &mut HeaderMap) {
    transport_headers(headers, anchored_version(), false);
}
#[cfg(any(feature = "ws", feature = "http_upgrade"))]
pub(crate) fn apply_websocket_headers(headers: &mut HeaderMap) {
    transport_headers(headers, anchored_version(), true);
}

#[cfg(test)]
#[path = "../tests/browser/mod.rs"]
mod tests;

#[cfg(feature = "grpc")]
pub(crate) fn grpc_user_agent(agent: &str) -> Option<String> {
    if agent == "golang" {
        return None;
    }
    if !matches!(agent, "" | "chrome" | "edge" | "firefox") {
        return Some(agent.to_owned());
    }
    let mut headers = HeaderMap::new();
    if !agent.is_empty() {
        headers.insert("user-agent", HeaderValue::from_str(agent).ok()?);
    }
    transport_headers(&mut headers, anchored_version(), false);
    headers
        .get("user-agent")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}
