//! Registration must not copy a large task through every admission frame.
use super::*;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

struct LargeTask {
    payload: [u8; 384 * 1024],
    release: oneshot::Receiver<()>,
}
impl Future for LargeTask {
    type Output = Result<(), zero_platform_tokio::contract::lifecycle::ComponentError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if Pin::new(&mut self.release).poll(cx).is_pending() {
            return Poll::Pending;
        }
        assert!(std::hint::black_box(&self.payload)
            .iter()
            .all(|byte| *byte == 7));
        Poll::Ready(Ok(()))
    }
}

#[test]
fn large_task_registration_keeps_original_join_on_default_worker_stack() {
    const CHILD: &str = "ZERO_SCOPE_LARGE_TASK_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "contract_scope::stack::large_task_registration_keeps_original_join_on_default_worker_stack", "--nocapture"])
            .env(CHILD, "1")
            .env_remove("RUST_MIN_STACK")
            .output().unwrap();
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("running 1 test"),
            "child must execute the original registration case"
        );
        assert!(
            output.status.success(),
            "original worker failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::spawn(async {
            let mut owner = scope(1);
            let (release, receive) = oneshot::channel();
            let (id, witness) = owner
                .registrar()
                .spawn_tracked(
                    TaskClass::Critical,
                    LargeTask {
                        payload: [7; 384 * 1024],
                        release: receive,
                    },
                )
                .unwrap();
            assert!(!witness.is_joined());
            release.send(()).unwrap();
            let exit = owner.next_exit().await.unwrap();
            assert_eq!(exit.id, id);
            exit.result.unwrap();
            let joined = witness.wait_registration_released().await;
            assert!(witness.matches_joined(&joined));
            joined.result().unwrap();
            assert!(owner
                .shutdown(Instant::now() + Duration::from_secs(1))
                .await
                .is_complete());
        })
        .await
        .unwrap();
    });
}
