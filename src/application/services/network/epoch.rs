use super::{
    stream::{Custody, EventStream},
    unique_candidates,
};
use std::{
    io,
    net::{SocketAddr, ToSocketAddrs},
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
use zero_connector::EventSinkTcpStream;
use zero_platform_tokio::contract::{
    lifecycle::{ChannelControl, CloseReport, TaskClass},
    scope::{TaskExit, TaskJoinWitness, TaskRegistrar, TaskScope},
    CancellationToken,
};
use zero_platform_tokio::{ControlledTcpControl, EgressInterfaceControl, PendingTcpConnect};
use zero_traits::DialPolicy;

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
    pub(super) async fn resolve(&self, host: String, port: u16) -> io::Result<Vec<SocketAddr>> {
        if self.closing.load(Ordering::Acquire) || !self.faults.lock().unwrap().is_empty() {
            return Err(io::Error::other("event generation closing"));
        }
        let job = {
            let _admission = self.admission.lock().unwrap();
            if self.closing.load(Ordering::Acquire) || !self.faults.lock().unwrap().is_empty() {
                return Err(io::Error::other("event generation closing"));
            }
            let job = Arc::new(AsyncMutex::new(Some(tokio::task::spawn_blocking(
                move || {
                    (host.as_str(), port)
                        .to_socket_addrs()
                        .map(unique_candidates)
                },
            ))));
            self.lookups.lock().unwrap().push(job.clone());
            job
        };
        let mut retained = job.lock().await;
        let result = retained
            .as_mut()
            .expect("original lookup retained")
            .await
            .map_err(io::Error::other)?;
        retained.take();
        drop(retained);
        self.lookups
            .lock()
            .unwrap()
            .retain(|value| !Arc::ptr_eq(value, &job));
        result
    }
    pub(super) async fn connect(
        self: &Arc<Self>,
        peer: SocketAddr,
        egress: &EgressInterfaceControl,
        deadline: Instant,
    ) -> io::Result<Box<dyn EventSinkTcpStream>> {
        if self.closing.load(Ordering::Acquire) || !self.faults.lock().unwrap().is_empty() {
            return Err(io::Error::other("event generation closing"));
        }
        self.reap().await?;
        let (original, witness, rx) = {
            let _admission = self.admission.lock().unwrap();
            if self.closing.load(Ordering::Acquire) || !self.faults.lock().unwrap().is_empty() {
                return Err(io::Error::other("event generation closing"));
            }
            if self.originals.lock().unwrap().len() >= 256 {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "event native connection capacity exhausted",
                ));
            }
            let selection = egress.select_for_peer(peer);
            selection.ensure_connectable()?;
            let prepared = PendingTcpConnect::prepare(
                peer,
                &DialPolicy::default(),
                &selection,
                self.stop.child_token(),
                None,
            );
            let (mut pending, control) = match prepared {
                Ok(value) => value,
                Err(source) => {
                    let unknown = source.stage() != "validate_dial_policy";
                    if unknown {
                        self.faults.lock().unwrap().push(io::Error::other(source));
                    }
                    return Err(io::Error::other(if unknown {
                        "event TCP preparation original disposition unknown"
                    } else {
                        "event TCP policy rejected"
                    }));
                }
            };
            let original = Arc::new(Original {
                control: AsyncMutex::new(control),
                custody: Arc::new(Custody::default()),
                receipt: Mutex::new(None),
            });
            // Original controller installed synchronously before any connect await.
            self.originals.lock().unwrap().push(original.clone());
            let custody = original.custody.clone();
            let (tx, rx) = oneshot::channel();
            let spawned = self.spawn_original(Box::pin(async move {
                let result = pending
                    .connect(deadline)
                    .await
                    .map(|stream| EventStream::new(stream, custody))
                    .map_err(io::Error::other);
                let _ = tx.send(result);
            }));
            let witness = match spawned {
                Ok(witness) => {
                    original.custody.add(witness.clone());
                    witness
                }
                Err(error) => {
                    original.custody.seal();
                    return Err(error);
                }
            };
            original.custody.seal();
            (original, witness, rx)
        };
        let result = rx
            .await
            .map_err(|_| io::Error::other("original event connect task stopped"))?;
        let joined = tokio::time::timeout_at(deadline, witness.wait())
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "event connect exit not joined")
            })?;
        if joined.result().is_err() {
            return Err(io::Error::other("original event connect task failed"));
        }
        match result {
            Ok(stream) => Ok(Box::new(stream)),
            Err(error) => {
                self.close_returned(deadline).await?;
                if original.receipt.lock().unwrap().is_none() {
                    return Err(io::Error::other(
                        "failed event candidate original not closed",
                    ));
                }
                Err(error)
            }
        }
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
