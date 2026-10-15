use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use super::*;

const MIB: usize = 1024 * 1024;

fn fixtures(dir: &Path) {
    let mut value = delivery("historical", 0);
    value.event.payload = serde_json::json!({"padding":"x".repeat(32*1024)});
    let mut pair = serde_json::to_vec(&JournalRecord::Put {
        delivery: Box::new(value),
    })
    .unwrap();
    pair.extend(b"\n{\"op\":\"ack\",\"sink_tag\":\"receiver\",\"event_id\":\"historical\"}\n");
    let mut history = BufWriter::new(File::create(dir.join("history.jsonl")).unwrap());
    for _ in 0..(512 * MIB / pair.len() + 1) {
        history.write_all(&pair).unwrap();
    }
    history.write_all(&put_frame("pending", 7)).unwrap();
    history.flush().unwrap();

    // A single record larger than the child's entire address-space allowance
    // catches hidden allocations by tagged enum decoding and compaction too.
    let mut huge = BufWriter::new(File::create(dir.join("huge.jsonl")).unwrap());
    let frame = put_frame("huge", 9);
    let marker = b"\"payload\":";
    let payload = frame
        .windows(marker.len())
        .position(|s| s == marker)
        .unwrap()
        + marker.len();
    huge.write_all(&frame[..payload]).unwrap();
    huge.write_all(b"\"").unwrap();
    let chunk = vec![b'x'; MIB];
    for _ in 0..256 {
        huge.write_all(&chunk).unwrap();
    }
    huge.write_all(b"\"").unwrap();
    let payload_bytes = serde_json::to_vec(&delivery("huge", 9).event.payload)
        .unwrap()
        .len();
    huge.write_all(&frame[payload + payload_bytes..]).unwrap();
    for _ in 0..1024 {
        huge.write_all(b"{\"op\":\"ack\",\"sink_tag\":\"receiver\",\"event_id\":\"unrelated\"}\n")
            .unwrap();
    }
    huge.write_all(b"partial tail").unwrap();
    huge.flush().unwrap();

    let mut dense = BufWriter::new(File::create(dir.join("dense.jsonl")).unwrap());
    let mut value = delivery("00000000-0000-4000-8000-000000000000", 0);
    value.event.payload = serde_json::json!({"padding":"x".repeat(1600)});
    let frame = serde_json::to_vec(&JournalRecord::Put {
        delivery: Box::new(value),
    })
    .unwrap();
    let id = b"00000000-0000-4000-8000-000000000000";
    let offset = frame.windows(id.len()).position(|s| s == id).unwrap();
    for index in 0..350_000 {
        dense.write_all(&frame[..offset]).unwrap();
        write!(dense, "00000000-0000-4000-8000-{index:012}").unwrap();
        dense.write_all(&frame[offset + id.len()..]).unwrap();
        dense.write_all(b"\n").unwrap();
    }
    dense.flush().unwrap();
}

fn verify(dir: &Path) {
    let path = dir.join("history.jsonl");
    let length = std::fs::metadata(&path).unwrap().len();
    assert!(length > (512 * MIB) as u64);
    let entries = std::fs::read_dir(dir).unwrap().count();
    let inspection = inspect_path(&path).unwrap();
    assert_eq!(inspection.pending, 1);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), length);
    assert_eq!(std::fs::read_dir(dir).unwrap().count(), entries);
    let outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 0, 0).unwrap();
    let pending = outbox.load_pending_excluding(&HashSet::new(), 2).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event.event_id, "pending");
    assert_eq!(pending[0].attempts, 7);
    assert!(std::fs::metadata(&path).unwrap().len() < 1024);
    drop(outbox);

    let path = dir.join("huge.jsonl");
    let length = std::fs::metadata(&path).unwrap().len();
    let inspection = inspect_path(&path).unwrap();
    assert_eq!(inspection.pending, 1);
    assert!(inspection.recoverable_partial_tail);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), length);
    let outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 0, 0).unwrap();
    let recovery = outbox.recovery_status().unwrap();
    assert_eq!(
        std::fs::metadata(recovery.preserved_path).unwrap().len(),
        length
    );
    assert_eq!(outbox.pending_counts().unwrap()["receiver"], 1);
    assert_eq!(outbox.journal_records, 1);
    // Compaction must retain the large unacknowledged payload, not shrink it
    // by dropping or replacing the event with its header.
    assert!(std::fs::metadata(&path).unwrap().len() > (256 * MIB) as u64);
    let inspection = inspect_path(&path).unwrap();
    assert_eq!(inspection.pending, 1);
    assert_eq!(inspection.records, 1);
    assert!(!inspection.recoverable_partial_tail);

    let dense = dir.join("dense.jsonl");
    let bytes = std::fs::metadata(&dense).unwrap().len();
    let inspection = inspect_path(&dense).unwrap();
    assert_eq!(inspection.pending, 350_000);
    assert_eq!(inspection.records, 350_000);
    assert_eq!(std::fs::metadata(&dense).unwrap().len(), bytes);
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        let peak_kib = status
            .lines()
            .find_map(|line| line.strip_prefix("VmHWM:"))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse::<usize>()
            .unwrap();
        assert!(
            // Genuine pending keys spill to the derived disk index. Payloads
            // and full in-memory key maps must not scale with the journal.
            peak_kib < 96 * 1024,
            "streaming scan peak RSS: {peak_kib} KiB"
        );
        eprintln!("streaming scan peak RSS: {peak_kib} KiB; 512 MiB journal and 256 MiB pending record preserved");
    }
}

#[test]
fn large_journal_inspection_recovery_and_compaction_preserve_pending_with_bounded_memory() {
    #[cfg(target_os = "linux")]
    if let Some(dir) = std::env::var_os("ZERO_STREAMING_FIXTURE_CHILD") {
        verify(Path::new(&dir));
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    fixtures(dir.path());
    #[cfg(not(target_os = "linux"))]
    verify(dir.path());
    #[cfg(target_os = "linux")]
    {
        use std::process::Command;
        use std::time::{Duration, Instant};
        let mut child = Command::new("sh")
            .args(["-c", "ulimit -v 196608 || exit 99; exec \"$1\" --exact dispatcher::outbox::tests::large::large_journal_inspection_recovery_and_compaction_preserve_pending_with_bounded_memory --nocapture --test-threads=1", "streaming-regression"])
            .arg(std::env::current_exe().unwrap())
            .env("ZERO_STREAMING_FIXTURE_CHILD", dir.path()).spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(300);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "192 MiB address-space regression failed: {status}"
                );
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("streaming regression exceeded five minutes");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}
