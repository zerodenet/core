//! Golden vectors generated from Xray-core v26.3.27 d2758a0 common/utils.
use super::*;
#[test]
fn cpu_seeded_release_schedule_matches_go_reference() {
    for (seed, time, version) in [
        (0i64, 1768262400u64, 143u32),
        (0i64, 1768262401u64, 144u32),
        (0i64, 1775000000u64, 146u32),
        (0i64, 1800000000u64, 154u32),
        (1i64, 1768262400u64, 143u32),
        (1i64, 1768262401u64, 144u32),
        (1i64, 1775000000u64, 145u32),
        (1i64, 1800000000u64, 154u32),
        (182i64, 1768262400u64, 143u32),
        (182i64, 1768262401u64, 144u32),
        (182i64, 1775000000u64, 145u32),
        (182i64, 1800000000u64, 154u32),
        (3640960168i64, 1768262400u64, 143u32),
        (3640960168i64, 1768262401u64, 144u32),
        (3640960168i64, 1775000000u64, 146u32),
        (3640960168i64, 1800000000u64, 154u32),
    ] {
        assert_eq!(
            chrome_version(seed, time),
            version,
            "seed={seed}, time={time}"
        );
    }
}
#[test]
fn browser_headers_match_official_fetch_values() {
    let cases: &[(&str, &[(&str, &str)])] = &[
        ("", &[
            ("user-agent", ""),
        ]),
        ("chrome", &[
            ("accept", "*/*"),
            ("accept-language", "en-US,en;q=0.9"),
            ("cache-control", "no-cache"),
            ("dnt", "1"),
            ("pragma", "no-cache"),
            ("priority", "u=1, i"),
            ("sec-ch-ua", "\"Not=A?Brand\";v=\"99\", \"Google Chrome\";v=\"151\", \"Chromium\";v=\"151\""),
            ("sec-ch-ua-mobile", "?0"),
            ("sec-ch-ua-platform", "\"Windows\""),
            ("sec-fetch-dest", "empty"),
            ("sec-fetch-mode", "cors"),
            ("sec-fetch-site", "same-origin"),
            ("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"),
        ]),
        ("custom", &[
            ("user-agent", "custom"),
        ]),
        ("default", &[
            ("accept", "*/*"),
            ("accept-language", "en-US,en;q=0.9"),
            ("cache-control", "no-cache"),
            ("dnt", "1"),
            ("pragma", "no-cache"),
            ("priority", "u=1, i"),
            ("sec-ch-ua", "\"Not=A?Brand\";v=\"99\", \"Google Chrome\";v=\"151\", \"Chromium\";v=\"151\""),
            ("sec-ch-ua-mobile", "?0"),
            ("sec-ch-ua-platform", "\"Windows\""),
            ("sec-fetch-dest", "empty"),
            ("sec-fetch-mode", "cors"),
            ("sec-fetch-site", "same-origin"),
            ("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"),
        ]),
        ("edge", &[
            ("accept", "*/*"),
            ("accept-language", "en-US,en;q=0.9"),
            ("cache-control", "no-cache"),
            ("dnt", "1"),
            ("pragma", "no-cache"),
            ("priority", "u=1, i"),
            ("sec-ch-ua", "\"Not=A?Brand\";v=\"99\", \"Microsoft Edge\";v=\"151\", \"Chromium\";v=\"151\""),
            ("sec-ch-ua-mobile", "?0"),
            ("sec-ch-ua-platform", "\"Windows\""),
            ("sec-fetch-dest", "empty"),
            ("sec-fetch-mode", "cors"),
            ("sec-fetch-site", "same-origin"),
            ("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36Edg/151.0.0.0"),
        ]),
        ("firefox", &[
            ("accept", "*/*"),
            ("accept-language", "en-US,en;q=0.5"),
            ("cache-control", "no-cache"),
            ("dnt", "1"),
            ("pragma", "no-cache"),
            ("priority", "u=4"),
            ("sec-fetch-dest", "empty"),
            ("sec-fetch-mode", "cors"),
            ("sec-fetch-site", "same-origin"),
            ("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0"),
        ]),
        ("golang", &[
            ("user-agent", "Go-http-client/1.1"),
        ]),
    ];
    for (name, expected) in cases {
        let mut headers = HeaderMap::new();
        if *name != "default" {
            set(&mut headers, "user-agent", name);
        }
        transport_headers(&mut headers, 151, false);
        assert_eq!(headers.len(), expected.len(), "{name}: {headers:?}");
        for (key, value) in *expected {
            assert_eq!(headers[*key], *value, "{name}: {key}");
        }
    }
}
#[test]
fn browser_defaults_preserve_explicit_request_policy() {
    for websocket in [false, true] {
        let mut headers = HeaderMap::new();
        for name in ["cache-control", "pragma", "accept", "priority"] {
            set(&mut headers, name, "custom");
        }
        transport_headers(&mut headers, 151, websocket);
        for name in ["cache-control", "pragma", "accept", "priority"] {
            assert_eq!(headers[name], "custom");
        }
        assert_eq!(
            headers["sec-fetch-mode"],
            if websocket { "websocket" } else { "cors" }
        );
    }
    let mut headers = HeaderMap::new();
    transport_headers(&mut headers, 151, true);
    assert!(!headers.contains_key("priority"));
}
