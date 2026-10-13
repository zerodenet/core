use super::*;
use serde::de::IgnoredAny;
use std::io::{Cursor, Write};

fn accepted(bytes: &[u8]) -> bool {
    let mut reader = BufReader::with_capacity(17, Cursor::new(bytes));
    read_record::<IgnoredAny>(&mut reader, &mut Vec::new())
        .unwrap()
        .unwrap()
        .parsed
        .unwrap()
        .is_ok()
}

#[test]
fn skipped_values_match_delivery_numbers_strings_and_depth_for_small_and_large_records() {
    let mut values = vec![
        "null".to_owned(),
        "true".to_owned(),
        "false".to_owned(),
        "0".to_owned(),
        "-0".to_owned(),
        "18446744073709551616".to_owned(),
        "1.7976931348623157e308".to_owned(),
        "-1.7976931348623157e308".to_owned(),
        "1e-9999".to_owned(),
        "0e9999".to_owned(),
        "1e999".to_owned(),
        "-1e999".to_owned(),
        "1.7976931348623159e308".to_owned(),
        r#""\uD800""#.to_owned(),
        r#""\uDC00""#.to_owned(),
        r#""\uD800\u0000""#.to_owned(),
        r#""\uD83D\uDE00""#.to_owned(),
        r#""\"\\\/\b\f\n\r\t\u0000中""#.to_owned(),
        "[1,]".to_owned(),
        r#"{"x":1,}"#.to_owned(),
        "true false".to_owned(),
    ];
    for depth in [120, 126, 127, 128, 160] {
        values.push(format!("{}0{}", "[".repeat(depth), "]".repeat(depth)));
    }
    for padding in [0, BUFFER_BYTES + 9] {
        for value in &values {
            let bytes = format!(r#"{{"padding":"{}","value":{value}}}"#, "x".repeat(padding));
            let expected = serde_json::from_slice::<serde_json::Value>(bytes.as_bytes()).is_ok();
            assert_eq!(
                accepted(bytes.as_bytes()),
                expected,
                "padding={padding}, {value}"
            );
        }
    }
}

#[test]
fn escape_validation_crosses_stream_buffers_and_does_not_hide_invalid_utf8() {
    for suffix in [r#"\uD83D\uDE00"#, r#"\uD800"#, r#"\uDC00"#, r#"\q"#] {
        for padding in [BUFFER_BYTES - 3, BUFFER_BYTES, BUFFER_BYTES + 3] {
            let bytes = format!("\"{}{suffix}\"\n", "x".repeat(padding));
            let expected = serde_json::from_slice::<serde_json::Value>(bytes.as_bytes()).is_ok();
            assert_eq!(
                accepted(bytes.as_bytes()),
                expected,
                "padding={padding}, {suffix}"
            );
        }
    }
    let mut bytes = format!("\"{}\"\n", "x".repeat(BUFFER_BYTES + 1)).into_bytes();
    bytes[BUFFER_BYTES] = 0xff;
    assert!(!accepted(&bytes));
}

#[test]
fn large_invalid_record_keeps_the_next_record_boundary() {
    let first = format!(
        r#"{{"padding":"{}","value":1e999}}"#,
        "x".repeat(BUFFER_BYTES)
    );
    let bytes = format!("{first}\n{{\"valid\":true}}\n");
    let mut reader = BufReader::new(Cursor::new(bytes.as_bytes()));
    let mut prefix = Vec::new();
    let failed = read_record::<IgnoredAny>(&mut reader, &mut prefix)
        .unwrap()
        .unwrap();
    assert!(failed.parsed.unwrap().is_err());
    assert_eq!(failed.bytes, first.len() as u64 + 1);
    assert!(failed.terminated);
    let next = read_record::<IgnoredAny>(&mut reader, &mut prefix)
        .unwrap()
        .unwrap();
    assert!(next.parsed.unwrap().is_ok());
    assert!(read_record::<IgnoredAny>(&mut reader, &mut prefix)
        .unwrap()
        .is_none());
}

#[test]
fn snapshot_revalidation_does_not_include_live_appends() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("live.jsonl");
    let first = format!("\"{}\"\n", "x".repeat(BUFFER_BYTES + 1));
    std::fs::write(&path, &first).unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let mut writer = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    writer.write_all(b"{\"later\":true}\n").unwrap();
    let mut reader = BufReader::new(Snapshot::new(file, first.len() as u64));
    let mut prefix = Vec::new();
    let record = read_record::<IgnoredAny>(&mut reader, &mut prefix)
        .unwrap()
        .unwrap();
    assert!(record.parsed.unwrap().is_ok());
    assert_eq!(record.bytes, first.len() as u64);
    assert!(read_record::<IgnoredAny>(&mut reader, &mut prefix)
        .unwrap()
        .is_none());
}
