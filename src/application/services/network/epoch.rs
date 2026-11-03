use super::stream::Custody;
use std::{
    io,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    sync::{oneshot, Mutex as AsyncMutex},
    time::Instant,
};
use zero_platform_tokio::contract::{
    lifecycle::{ChannelControl, CloseReport, TaskClass},
    scope::{TaskExit, TaskJoinWitness, TaskRegistrar, TaskScope},
    CancellationToken,
};
use zero_platform_tokio::ControlledTcpControl;

pub(super) type EventTask =
    std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>;
type Lookup = Arc<AsyncMutex<Option<tokio::task::JoinHandle<io::Result<Vec<SocketAddr>>>>>>;
tokio::task_local! { pub(super) static ACTUAL_TASK: TaskJoinWitness; }
struct Original {
    control: AsyncMutex<ControlledTcpControl>,
    custody: Arc<Custody>,
    receipt: Mutex<Option<CloseReport>>,
}
pub(super) struct Epoch {
    pub(super) generation: u64,
    registrar: TaskRegistrar,
    stop: CancellationToken,
    scope: AsyncMutex<TaskScope>,
    originals: Mutex<Vec<Arc<Original>>>,
    lookups: Mutex<Vec<Lookup>>,
    failures: Mutex<Vec<TaskExit>>,
    admission: Mutex<()>,
    task_reports: Mutex<Vec<CloseReport>>,
    faults: Mutex<Vec<io::Error>>,
    closing: AtomicBool,
    confirmed: AtomicBool,
}
impl Epoch {
    pub(super) fn new(generation: u64) -> Arc<Self> {
        let scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1024).unwrap(),
        );
        Arc::new(Self {
            generation,
            registrar: scope.registrar(),
            stop: CancellationToken::new(),
            scope: AsyncMutex::new(scope),
            originals: Mutex::new(Vec::new()),
            lookups: Mutex::new(Vec::new()),
            failures: Mutex::new(Vec::new()),
            admission: Mutex::new(()),
            task_reports: Mutex::new(Vec::new()),
            faults: Mutex::new(Vec::new()),
            closing: AtomicBool::new(false),
            confirmed: AtomicBool::new(false),
        })
    }
    pub(super) fn closed(&self) -> bool {
        self.confirmed.load(Ordering::Acquire)
    }
    pub(super) fn spawn(&self, task: EventTask) -> io::Result<TaskJoinWitness> {
        let _admission = self.admission.lock().unwrap();
        self.spawn_original(task)
    }
    fn spawn_original(&self, task: EventTask) -> io::Result<TaskJoinWitness> {
        if self.closing.load(Ordering::Acquire) || !self.faults.lock().unwrap().is_empty() {
            return Err(io::Error::other("event generation closing"));
        }
        let (tx, rx) = oneshot::channel();
        let cancel = self.stop.child_token();
        let (_, witness) =
            self.registrar
                .spawn_tracked(TaskClass::Auxiliary, async move {
                    if let Ok(witness) = rx.await {
                        ACTUAL_TASK.scope(witness, async move {
                    tokio::select! { biased; _ = cancel.cancelled() => {}, _ = task => {} }
                }).await;
                    }
                    Ok(())
                })
                .map_err(io::Error::other)?;
        tx.send(witness.clone())
            .map_err(|_| io::Error::other("event task startup stopped"))?;
        Ok(witness)
    }
    fn record_exit(&self, exit: TaskExit) {
        if exit.result.is_err() {
            self.failures.lock().unwrap().push(exit);
        }
    }
    pub(super) async fn drive(&self) {
        loop {
            let exit = self.scope.lock().await.next_exit().await;
            match exit {
                Some(exit) => self.record_exit(exit),
                None => return,
            }
        }
    }
    pub(super) async fn reap(&self) -> io::Result<()> {
        // Another live publish may currently consume exits. It alone owns that
        // join; never wait for that lock from a task inside this same scope.
        if let Ok(mut scope) = self.scope.try_lock() {
            loop {
                tokio::select! { biased;
                    exit = scope.next_exit() => match exit { Some(exit) => self.record_exit(exit), None => break },
                    _ = std::future::ready(()) => break,
                }
            }
        }
        self.close_returned(Instant::now() + std::time::Duration::from_secs(1))
            .await
    }
    async fn close_returned(&self, deadline: Instant) -> io::Result<()> {
        let originals = self.originals.lock().unwrap().clone();
        for original in originals {
            if original.receipt.lock().unwrap().is_some() {
                continue;
            }
            if !original.custody.returned_and_joined() {
                continue;
            }
            let mut control = original.control.lock().await;
            if original.receipt.lock().unwrap().is_some() {
                continue;
            }
            let report = control
                .wait_closed(deadline)
                .await
                .map_err(io::Error::other)?;
            if !report.is_complete() {
                return Err(io::Error::other("event TCP original close unfinished"));
            }
            *original.receipt.lock().unwrap() = Some(report);
        }
        // Only the genuine retained controller receipt permits capacity reuse.
        self.originals
            .lock()
            .unwrap()
            .retain(|original| original.receipt.lock().unwrap().is_none());
        Ok(())
    }
    pub(super) async fn retire(&self, deadline: Instant) -> io::Result<()> {
        {
            let _admission = self.admission.lock().unwrap();
            self.closing.store(true, Ordering::Release);
        }
        self.stop.cancel();
        let report = self.scope.lock().await.shutdown(deadline).await;
        if !report.is_complete() {
            return Err(io::Error::other("event HTTP tasks not joined"));
        }
        if !report.failed_tasks.is_empty() {
            self.task_reports.lock().unwrap().push(report);
        }
        let jobs = self.lookups.lock().unwrap().clone();
        for job in jobs {
            let mut original = job.lock().await;
            if let Some(handle) = original.as_mut() {
                let joined = tokio::time::timeout_at(deadline, handle)
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "original event lookup not joined")
                    })?;
                // A resolver's ordinary io::Error is a completed lookup, while
                // an actual blocking-task panic remains retained as a fault.
                if let Err(source) = joined {
                    self.faults.lock().unwrap().push(io::Error::other(source));
                }
                original.take();
            }
        }
        self.lookups.lock().unwrap().clear();
        self.close_returned(deadline).await?;
        if !self.originals.lock().unwrap().is_empty() {
            return Err(io::Error::other(
                "event transferred views or original native close still pending",
            ));
        }
        if !self.task_reports.lock().unwrap().is_empty()
            || !self.failures.lock().unwrap().is_empty()
            || !self.faults.lock().unwrap().is_empty()
        {
            return Err(io::Error::other(
                "event HTTP original task failed; original failure retained after cleanup",
            ));
        }
        self.confirmed.store(true, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod tests;

mod dial;
