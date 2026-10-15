//! Isolated native + Rust heap evidence; fixtures stay below 32 MiB.
use super::*;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::time::Duration;

const CHILD: &str = "ZERO_BACKLOG_MEMORY_CHILD";
const NAME: &str = "dispatcher::memory::tests::backlog::pending_index_and_compaction_heap_do_not_scale_with_backlog";

fn fixture(path: &std::path::Path, count: usize) {
    let mut output = BufWriter::new(std::fs::File::create(path).unwrap());
    for id in 0..count {
        let event = ApiEvent::new(
            format!("{id:08}"),
            "flow.completed",
            1,
            serde_json::json!({"id":id}),
        );
        serde_json::to_writer(&mut output, &serde_json::json!({
            "op":"put","delivery":{"sink_tag":"receiver","event":event,"attempts":7,"message":"retry"}
        })).unwrap();
        output.write_all(b"\n").unwrap();
    }
    // Trigger real startup compaction, including its second derived index.
    for _ in 0..(3 * count + 65) {
        output
            .write_all(b"{\"op\":\"ack\",\"sink_tag\":\"receiver\",\"event_id\":\"unknown\"}\n")
            .unwrap();
    }
    output.flush().unwrap();
}

fn measure(count: usize) -> (usize, i64) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.jsonl");
    fixture(&path, count);
    let before = std::fs::metadata(&path).unwrap().len();
    // SQLite uses C allocations, invisible to Rust's GlobalAlloc tracker.
    // This executable runs only this child test, so its native high-water
    // reset cannot interfere with concurrent database users in other tests.
    let native_base = unsafe { rusqlite::ffi::sqlite3_memory_used() };
    unsafe {
        rusqlite::ffi::sqlite3_memory_highwater(1);
    }
    let (outbox, rust_peak) =
        heap::measure(|| DeliveryOutbox::open(path.to_str().unwrap(), None, 0, 0).unwrap());
    let native_peak = unsafe { rusqlite::ffi::sqlite3_memory_highwater(0) } - native_base;
    assert!(
        native_peak > 0,
        "native allocator telemetry must be available"
    );
    assert_eq!(outbox.pending_counts().unwrap()["receiver"], count);
    assert!(std::fs::metadata(&path).unwrap().len() < before);
    assert!(
        rust_peak < 1024 * 1024,
        "{count} events Rust heap {rust_peak}"
    );
    assert!(
        native_peak < 2 * 1024 * 1024,
        "{count} events native heap {native_peak}"
    );
    // Streaming verification of every ID and payload in the compacted file.
    let mut found = 0;
    let mut line = String::new();
    let mut reader = BufReader::new(std::fs::File::open(&path).unwrap());
    while reader.read_line(&mut line).unwrap() != 0 {
        let value: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(
            value["delivery"]["event"]["event_id"],
            format!("{found:08}")
        );
        assert_eq!(value["delivery"]["event"]["payload"]["id"], found);
        assert_eq!(value["delivery"]["attempts"], 7);
        found += 1;
        line.clear();
    }
    assert_eq!(found, count);
    eprintln!(
        "backlog {count}: Rust extra heap {rust_peak}, SQLite extra heap {native_peak}; not RSS"
    );
    (rust_peak, native_peak)
}

#[test]
fn pending_index_and_compaction_heap_do_not_scale_with_backlog() {
    if std::env::var_os(CHILD).is_some() {
        let small = measure(4000);
        let large = measure(40000);
        assert!(
            large.0 <= small.0 + 256 * 1024,
            "Rust heap grew: {small:?} -> {large:?}"
        );
        assert!(
            large.1 <= small.1 + 512 * 1024,
            "native heap grew: {small:?} -> {large:?}"
        );
        return;
    }
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", NAME, "--nocapture", "--test-threads=1"])
        .env(CHILD, "1")
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(
                status.success(),
                "isolated backlog regression failed: {status}"
            );
            break;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("backlog regression exceeded two minutes");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
