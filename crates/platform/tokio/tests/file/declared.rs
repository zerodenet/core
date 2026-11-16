use super::*;
use std::{future::Future, task::Poll};
use tokio::time::{timeout, Duration};
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("zero-declared-file-{}-{}", std::process::id(), {
                static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            }));
        std::fs::write(&path, b"first").unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).unwrap();
    }
}
#[tokio::test]
async fn repeatable_grant_closes_each_original_and_recovers_after_a_bounded_read_failure() {
    let file = Fixture::new();
    let reads = DeclaredFileReads::new([(file.0.clone(), 16)], CancellationToken::new()).unwrap();
    reads.activate();
    assert_eq!(reads.read(file.0.clone(), 16).await.unwrap(), b"first");
    std::fs::write(&file.0, b"replacement").unwrap();
    assert_eq!(
        reads.read(file.0.clone(), 16).await.unwrap(),
        b"replacement"
    );
    assert!(reads.read(file.0.clone(), 3).await.is_err());
    std::fs::write(&file.0, b"good").unwrap();
    assert_eq!(reads.read(file.0.clone(), 16).await.unwrap(), b"good");
    let slot = reads.slots[&file.0].lock().await;
    assert_eq!((slot.closed, slot.joined), (4, 4));
    assert!(slot.original.is_none() && slot.job.is_none());
    drop(slot);
    let receipt = reads
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert!(
        receipt.is_complete()
            && !receipt.closed_io.is_empty()
            && !receipt.released_resources.is_empty()
    );
    assert!(reads.read(file.0.clone(), 16).await.is_err());
}
#[tokio::test]
async fn cancelled_wait_keeps_the_actual_open_file_job_until_its_join_and_native_close() {
    timeout(Duration::from_secs(5), async {
        let file = Fixture::new();
        let reads =
            DeclaredFileReads::new([(file.0.clone(), 16)], CancellationToken::new()).unwrap();
        reads.activate();
        let original = Arc::new(PendingFileRead::new(file.0.clone(), 16).unwrap());
        let (opened, ready) = tokio::sync::oneshot::channel();
        let (release, waiting) = std::sync::mpsc::channel();
        let actual = original.clone();
        {
            let mut slot = reads.slots[&file.0].lock().await;
            slot.original = Some(original.clone());
            slot.job = Some(tokio::task::spawn_blocking(move || {
                actual.run_with_after_open(|| {
                    opened.send(()).unwrap();
                    waiting.recv().unwrap();
                })
            }));
        }
        ready.await.unwrap();
        // The native File is installed inside that real still-running job.
        // Poll the exact provider wait while its original join is pending.
        let mut read = Box::pin(reads.read(file.0.clone(), 16));
        std::future::poll_fn(|cx| {
            assert!(read.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(read);
        {
            let slot = reads.slots[&file.0].lock().await;
            assert!(Arc::ptr_eq(slot.original.as_ref().unwrap(), &original));
            assert!(!slot.job.as_ref().unwrap().is_finished());
        }
        let pending = reads.shutdown(Instant::now()).await.unwrap();
        assert!(!pending.is_complete() && pending.closed_io.is_empty());
        release.send(()).unwrap();
        let closed = reads
            .shutdown(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        assert!(closed.is_complete());
        assert!(!closed.closed_io.is_empty());
        assert!(matches!(
            original.evidence().native,
            FileNativeEvidence::ConfirmedClosed
        ));
        let slot = reads.slots[&file.0].lock().await;
        assert_eq!((slot.closed, slot.joined), (1, 1));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_join_panic_keeps_the_original_failure_and_positive_native_close_separate() {
    let file = Fixture::new();
    let instance = zero_core::component::ComponentInstanceId {
        component: zero_core::component::ComponentId(17),
        generation: zero_core::component::ResourceGeneration(1),
    };
    let reads = DeclaredFileReads::for_component(
        instance,
        [(file.0.clone(), 16)],
        CancellationToken::new(),
    )
    .unwrap();
    reads.activate();
    let original = Arc::new(PendingFileRead::new(file.0.clone(), 16).unwrap());
    let worker = original.clone();
    {
        let mut slot = reads.slots[&file.0].lock().await;
        slot.original = Some(original.clone());
        slot.job = Some(tokio::task::spawn_blocking(move || {
            worker.run_with_after_open(|| std::panic::panic_any("original open-file panic"))
        }));
    }
    let report = reads
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert!(report.is_complete() && !report.closed_io.is_empty());
    assert_eq!(report.resource_failures.len(), 1);
    assert!(
        report.failed_tasks.is_empty(),
        "native blocking join has no invented Scope TaskId"
    );
    let fault = &report.resource_failures[0];
    assert_eq!(fault.instance, instance);
    assert!(!fault.ownership_received());
    assert!(!fault.reported());
    let slot = reads.slots[&file.0].lock().await;
    assert!(Arc::ptr_eq(
        fault.source.library_arc().unwrap(),
        slot.failure.as_ref().unwrap()
    ));
    assert!(matches!(
        original.evidence().native,
        FileNativeEvidence::ConfirmedClosed
    ));
    drop(slot);
    let again = reads
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert!(
        Arc::ptr_eq(&again.resource_failures[0], fault),
        "same original failure, not a replacement error or automatic ACK"
    );
}
