use super::stream::Custody;
use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::{Mutex as AsyncMutex, oneshot},
    time::Instant,
};
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    CancellationToken,
    lifecycle::{CloseReport, TaskClass},
    scope::{TaskExit, TaskJoinWitness, TaskRegistrar, TaskScope},
};
use zero_platform_tokio::contract::{component::ResourceDeclaration, lifecycle::ResourceLifecycle};
use zero_platform_tokio::controlled_carrier::{
    CarrierAdoption, CarrierOriginalTotals, CarrierOwner, CarrierOwnerBinding,
};
use zero_traits::DialPolicy;

pub(super) type EventTask =
    std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>;
mod failure;
mod lookup;
use lookup::Lookup;
pub(super) type EpochLimits = zero_platform_tokio::contract::network::EventNetworkLimits;
tokio::task_local! { pub(super) static ACTUAL_TASK: TaskJoinWitness; }
struct Original {
    adoption: CarrierAdoption,
    close_fault: Mutex<Option<Arc<io::Error>>>,
    operation_fault: Mutex<Option<Arc<io::Error>>>,
    closing: AsyncMutex<()>,
    custody: Arc<Custody>,
    receipt: Mutex<Option<CloseReport>>,
}
pub(super) struct Epoch {
    pub(super) generation: u64,
    registrar: TaskRegistrar,
    limits: EpochLimits,
    stop: Arc<CancellationToken>,
    native: AsyncMutex<CarrierOwner>,
    binding: CarrierOwnerBinding,
    scope: AsyncMutex<TaskScope>,
    originals: Mutex<Vec<Arc<Original>>>,
    lookups: Mutex<Vec<Arc<Lookup>>>,
    failures: Mutex<Vec<Arc<TaskExit>>>,
    admission: Mutex<()>,
    task_reports: Mutex<Vec<Arc<CloseReport>>>,
    faults: Mutex<Vec<Arc<io::Error>>>,
    closing: AtomicBool,
    confirmed: AtomicBool,
    failed: AtomicBool,
}
impl Epoch {
    pub(super) fn new(
        generation: u64,
        limits: EpochLimits,
        instance: ComponentInstanceId,
        totals: Arc<CarrierOriginalTotals>,
    ) -> io::Result<Arc<Self>> {
        let scope = TaskScope::new(tokio::runtime::Handle::current(), limits.max_tasks());
        let stop = Arc::new(CancellationToken::new());
        let native = CarrierOwner::new(instance, limits.max_originals(), &stop);
        let binding = native
            .registration()
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledDirectStream {
                    dial: DialPolicy::default(),
                    max_sockets: limits.max_originals(),
                },
                &scope.registrar(),
                &stop,
                &totals,
            )
            .map_err(io::Error::other)?;
        Ok(Arc::new(Self {
            generation,
            registrar: scope.registrar(),
            limits,
            stop,
            native: AsyncMutex::new(native),
            binding,
            scope: AsyncMutex::new(scope),
            originals: Mutex::new(Vec::new()),
            lookups: Mutex::new(Vec::new()),
            failures: Mutex::new(Vec::new()),
            admission: Mutex::new(()),
            task_reports: Mutex::new(Vec::new()),
            faults: Mutex::new(Vec::new()),
            closing: AtomicBool::new(false),
            confirmed: AtomicBool::new(false),
            failed: AtomicBool::new(false),
        }))
    }
    pub(super) fn closed(&self) -> bool {
        self.confirmed.load(Ordering::Acquire)
    }
    pub(super) fn spawn(&self, task: EventTask) -> io::Result<TaskJoinWitness> {
        let _admission = self.admission.lock().unwrap();
        self.spawn_original(task)
    }
    fn spawn_original(&self, task: EventTask) -> io::Result<TaskJoinWitness> {
        if self.closing.load(Ordering::Acquire)
            || self.failed.load(Ordering::Acquire)
            || !self.faults.lock().unwrap().is_empty()
        {
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
            self.failed.store(true, Ordering::Release);
            self.failures.lock().unwrap().push(Arc::new(exit));
        }
        self.reap_lookups();
    }
    pub(super) async fn drive(&self) {
        loop {
            let exit = self.scope.lock().await.next_exit().await;
            match exit {
                Some(exit) => {
                    self.record_exit(exit);
                    let _ = self
                        .close_returned(Instant::now() + std::time::Duration::from_secs(1))
                        .await;
                }
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
        for original in &originals {
            if original.receipt.lock().unwrap().is_some() {
                continue;
            }
            if !original.custody.returned_and_joined() {
                continue;
            }
            let _closing = match tokio::time::timeout_at(deadline, original.closing.lock()).await {
                Ok(guard) => guard,
                Err(_) => continue,
            };
            if original.receipt.lock().unwrap().is_some() {
                continue;
            }
            let report = match original.adoption.retire(deadline).await {
                Ok(report) => report,
                Err(source) => {
                    self.failed.store(true, Ordering::Release);
                    original
                        .close_fault
                        .lock()
                        .unwrap()
                        .get_or_insert_with(|| Arc::new(io::Error::other(source)));
                    continue;
                }
            };
            if !report.is_complete()
                || !original
                    .adoption
                    .closed()
                    .is_some_and(|closed| closed.matches(&original.adoption))
                || !original
                    .adoption
                    .users_closed()
                    .is_some_and(|closed| closed.matches(&original.adoption))
            {
                continue;
            }
            *original.receipt.lock().unwrap() = Some(report);
        }
        // Only the genuine retained controller receipt permits capacity reuse.
        let retired = {
            let mut originals = self.originals.lock().unwrap();
            let mut retired = Vec::new();
            let mut index = 0;
            while index < originals.len() {
                if originals[index].receipt.lock().unwrap().is_some() {
                    retired.push(originals.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            retired
        };
        for original in &retired {
            if let Some(source) = original.close_fault.lock().unwrap().as_ref() {
                self.faults
                    .lock()
                    .unwrap()
                    .push(failure::attempt_failure(original, source.clone()));
            }
        }
        drop(retired);
        if self.originals.lock().unwrap().iter().any(|original| {
            original.custody.returned_and_joined() && original.receipt.lock().unwrap().is_none()
        }) {
            return Err(io::Error::other(
                "returned event candidate retains its actual native/user cleanup",
            ));
        }
        Ok(())
    }
    pub(super) async fn retire(self: &Arc<Self>, deadline: Instant) -> io::Result<()> {
        {
            let _admission = self.admission.lock().unwrap();
            self.closing.store(true, Ordering::Release);
        }
        self.stop.cancel();
        let report = self.scope.lock().await.shutdown(deadline).await;
        let complete = report.is_complete();
        if !report.failed_tasks.is_empty() {
            self.failed.store(true, Ordering::Release);
            self.task_reports.lock().unwrap().push(Arc::new(report));
        }
        if !complete {
            return Err(io::Error::other("event HTTP tasks not joined"));
        }
        self.reap_lookups();
        if !self.lookups.lock().unwrap().is_empty() {
            return Err(io::Error::other(
                "original event lookup caller or Scope receipt remains retained",
            ));
        }
        self.close_returned(deadline).await?;
        if !self.originals.lock().unwrap().is_empty() {
            return Err(io::Error::other(
                "event transferred views or original native close still pending",
            ));
        }
        let mut native = self.native.lock().await;
        let report = native.shutdown(deadline).await.map_err(io::Error::other)?;
        let registration = native.registration();
        if !report.is_complete()
            || !registration
                .closed()
                .is_some_and(|closed| closed.matches(&registration))
            || !registration
                .users_closed()
                .is_some_and(|closed| closed.matches(&registration))
        {
            return Err(io::Error::other(
                "actual event Carrier owner remains unconfirmed",
            ));
        }
        drop(native);
        self.confirmed.store(true, Ordering::Release);
        if let Some(error) = self.retained_failure() {
            return Err(error);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

mod dial;
