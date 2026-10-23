use super::*;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Condvar,
};
pub(super) struct Directory(pub(super) PathBuf);
impl Directory {
    pub(super) fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "zero-owned-file-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
#[derive(Default)]
pub(super) struct Gate {
    entered: tokio::sync::Notify,
    ready: Mutex<bool>,
    release: Condvar,
}
impl Gate {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub(super) fn hook(self: &Arc<Self>) -> Arc<dyn Fn() + Send + Sync> {
        let gate = self.clone();
        Arc::new(move || {
            gate.entered.notify_one();
            let mut ready = gate.ready.lock().unwrap();
            while !*ready {
                ready = gate.release.wait(ready).unwrap();
            }
        })
    }
    pub(super) async fn entered(&self) {
        tokio::time::timeout_at(deadline(), self.entered.notified())
            .await
            .unwrap();
    }
    pub(super) fn release(&self) {
        *self.ready.lock().unwrap() = true;
        self.release.notify_all();
    }
    pub(super) fn release_guard(self: &Arc<Self>) -> Release {
        Release(self.clone())
    }
}
pub(super) struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
