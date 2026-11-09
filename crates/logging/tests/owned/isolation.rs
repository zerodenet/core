use super::*;
use std::io::Read;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_actual_fifo_output_does_not_block_another_original_file_or_main() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let dir = tempfile::tempdir().unwrap();
        let pipe = dir.path().join("blocked.log");
        let ordinary = dir.path().join("ordinary.log");
        // Real kernel FIFO, not a pending callback or synthetic native proof.
        assert!(std::process::Command::new("mkfifo")
            .arg(&pipe)
            .status()
            .unwrap()
            .success());
        let mut reader = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&pipe)
            .unwrap();
        let mut config = config(&pipe);
        config.files[0].max_bytes = u64::MAX;
        let mut second = config.files[0].clone();
        second.path = ordinary.to_str().unwrap().into();
        config.files.push(second);
        let (mut scope, mut owner) = prepare(&config);
        initialized(&mut scope, &mut owner).await;
        let mut blocked = owner.writer(0).unwrap();
        let mut other = owner.writer(1).unwrap();
        let payload = vec![b'x'; 512 * 1024];
        blocked.write_all(&payload).unwrap();
        owner.drive(&scope.registrar()).unwrap();
        let mut first = [0u8; 1];
        reader.read_exact(&mut first).unwrap();
        assert_eq!(first, [b'x']);
        assert!(
            !owner.witness(0).unwrap().is_joined(),
            "real write exceeds the kernel pipe capacity"
        );
        other.write_all(b"business-ack\n").unwrap();
        until(&mut scope, &mut owner, |owner| {
            owner.usage().pending_bytes == payload.len()
                && fs::read(&ordinary).is_ok_and(|bytes| bytes == b"business-ack\n")
        })
        .await;
        assert_eq!(owner.usage().pending_bytes, payload.len());
        assert!(!owner.drained(), "the original blocked IO remains owned");
        // Test peer consumes the exact rest; no production timeout or budget is
        // changed to free the real worker. The original job then actually exits.
        let mut remaining = vec![0u8; payload.len() - 1];
        reader.read_exact(&mut remaining).unwrap();
        assert!(remaining.iter().all(|byte| *byte == b'x'));
        until(&mut scope, &mut owner, ScopedTracing::drained).await;
        assert_eq!(owner.usage().pending_bytes, 0);
        retire(&mut scope, &mut owner).await;
    })
    .await
    .unwrap();
}
