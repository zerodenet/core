use super::{
    failure,
    physical::{Body, Opening},
    Driver, Physical, PoolOptions,
};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::{sync::Notify, time::Instant};
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::{
    contract::{
        network::{CarrierContext, PendingCarrierObserver, UpstreamBinding, UpstreamTransport},
        scope::{TaskClass, TaskRegistrar},
        CancellationToken, Reason, Stage,
    },
    controlled_carrier::{CarrierGrants, CarrierIo},
};
pub struct ManagedPool {
    pub(super) instance: ComponentInstanceId,
    pub(super) options: Arc<PoolOptions>,
    pub(super) upstream: UpstreamTransport,
    pub(super) tasks: TaskRegistrar,
    pub(super) grants: CarrierGrants,
    pub(super) cancel: CancellationToken,
    pub(super) ready: AtomicBool,
    pub(super) records: Mutex<Vec<Arc<Physical>>>,
    pub(super) changed: Notify,
    pub(super) connecting: tokio::sync::Mutex<()>,
    pub(super) epoch: Mutex<Option<super::epoch::Epoch>>,
    pub(super) settled: Mutex<zero_platform_tokio::contract::lifecycle::CloseReport>,
    pub(super) operations: AtomicUsize,
}
impl ManagedPool {
    pub fn operation(self: &Arc<Self>) -> Result<Operation, crate::RuntimeError> {
        if !self.ready() {
            return Err(std::io::Error::other(failure(
                self.instance,
                Stage::Prepare,
                Reason::Revoked,
            ))
            .into());
        }
        let mut current = self.operations.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(1).ok_or_else(|| {
                std::io::Error::other(failure(self.instance, Stage::Prepare, Reason::Capacity))
            })?;
            match self.operations.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        Ok(Operation(self.clone()))
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    pub async fn serialize(&self) -> Result<tokio::sync::MutexGuard<'_, ()>, crate::RuntimeError> {
        tokio::select! { biased;
            _ = self.cancel.cancelled() => Err(std::io::Error::other(failure(self.instance, Stage::Prepare, Reason::Revoked)).into()),
            result = tokio::time::timeout(self.options.open_budget, self.connecting.lock()) => result.map_err(|source| std::io::Error::new(std::io::ErrorKind::TimedOut, source).into()),
        }
    }
    pub(super) fn ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
            && !self.cancel.is_cancelled()
            && !self.tasks.cancellation().is_cancelled()
    }
    pub(super) fn stop(&self) {
        self.ready.store(false, Ordering::Release);
        self.cancel.cancel();
        self.changed.notify_waiters();
    }
    pub fn check(&self, profile: &str) -> Result<(), crate::RuntimeError> {
        if !self.ready() || self.options.profile.as_ref() != profile {
            return Err(std::io::Error::other(failure(
                self.instance,
                Stage::Prepare,
                Reason::StalePlan,
            ))
            .into());
        }
        Ok(())
    }
    /// Fixed resourceful library callers retain inline setup. Their original
    /// protocol driver still belongs to their actual supplied TaskScope.
    pub async fn open_owned<C, F, Fut>(
        self: &Arc<Self>,
        profile: &str,
        binding: &UpstreamBinding,
        prepare: F,
    ) -> Result<(C, Arc<Physical>), crate::RuntimeError>
    where
        C: Send + 'static,
        F: FnOnce(CarrierIo) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(C, Driver), crate::RuntimeError>> + Send + 'static,
    {
        self.open_observed(
            profile,
            binding,
            CarrierContext {
                session: None,
                deadline: None,
                cancel: self.cancel.child_token(),
            },
            None,
            prepare,
        )
        .await
    }
    /// Actual protocol caller supplies its cancellation/deadline and weak
    /// waiting ledger. The pool owns the separate BackgroundControl setup task;
    /// a successfully installed shared driver never inherits the first caller.
    pub async fn open_observed<C, F, Fut>(
        self: &Arc<Self>,
        profile: &str,
        binding: &UpstreamBinding,
        mut call: CarrierContext,
        waiting: Option<PendingCarrierObserver>,
        prepare: F,
    ) -> Result<(C, Arc<Physical>), crate::RuntimeError>
    where
        C: Send + 'static,
        F: FnOnce(CarrierIo) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(C, Driver), crate::RuntimeError>> + Send + 'static,
    {
        self.check(profile)?;
        let rejected = if call.cancel.is_cancelled() {
            Some(Reason::Cancelled)
        } else if call.deadline.is_some_and(|end| Instant::now() >= end) {
            Some(Reason::Timeout)
        } else {
            None
        };
        if let Some(reason) = rejected {
            let mut error = failure(self.instance, Stage::Prepare, reason);
            error.delivery = zero_platform_tokio::contract::DeliveryState::NotSubmitted;
            return Err(std::io::Error::other(error).into());
        }
        let deadline = Instant::now()
            .checked_add(self.options.open_budget)
            .ok_or_else(|| std::io::Error::other("carrier deadline overflow"))?;
        call.deadline = Some(
            call.deadline
                .map_or(deadline, |actual| actual.min(deadline)),
        );
        let deadline = call.deadline.unwrap();
        let candidates = binding.prefix.is_empty().then(|| {
            super::candidates::Candidates::new(CarrierContext {
                session: None,
                deadline: call.deadline,
                cancel: call.cancel.clone(),
            })
        });
        let prepare = Arc::new(Mutex::new(Some(prepare)));
        loop {
            self.reap_ready().map_err(std::io::Error::other)?;
            if self.records.lock().unwrap().len() >= self.options.limit.get() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "live or retiring carrier pool is full",
                )
                .into());
            }
            let mut grant = self.grants.reserve().map_err(std::io::Error::other)?;
            let view = grant.claim_driver_view().map_err(std::io::Error::other)?;
            let record = Physical::new(
                grant.adoption(),
                grant.cancellation(),
                view,
                self,
                binding.clone(),
            );
            self.records.lock().unwrap().push(record.clone());
            let opening = Opening(record.clone());
            let waiting = waiting
                .clone()
                .or_else(|| self.upstream.observed_pending_carrier());
            if let Some(waiting) = &waiting {
                waiting
                    .retain(record.original.clone())
                    .map_err(std::io::Error::other)?;
            }
            let owner = self.clone();
            let original = record.clone();
            let selected = binding.clone();
            let context = call.clone();
            let actual_prepare = prepare.clone();
            let actual_candidates = candidates.clone();
            let operation = move |caller| {
                owner.prepare_original(
                    original,
                    selected,
                    context,
                    grant,
                    waiting,
                    caller,
                    actual_candidates,
                    actual_prepare,
                )
            };
            let preparing = async {
                if self.options.scoped_preparation {
                    super::setup::run(self, &record, move |caller| operation(Some(caller))).await
                } else {
                    operation(None).await
                }
            };
            let scope_cancel = self.tasks.cancellation();
            let result = tokio::select! {biased;
                _=self.cancel.cancelled()=>Err(failure(self.instance,Stage::Prepare,Reason::Revoked)),
                _=scope_cancel.cancelled()=>Err(failure(self.instance,Stage::Prepare,Reason::Revoked)),
                _=call.cancel.cancelled()=>Err(failure(self.instance,Stage::Prepare,Reason::Cancelled)),
                result=tokio::time::timeout_at(deadline,preparing)=>result.unwrap_or_else(|source|Err(failure(self.instance,Stage::Prepare,Reason::Timeout).with_source(source))),
            };
            match result {
                Ok(tagged) => {
                    let (connection, driver) = tagged.split(|parts| parts);
                    let body = Body(record.clone());
                    let cancel = record.stop.clone();
                    let (_, witness) = record
                        .original
                        .spawn_driver(
                            &self.tasks,
                            TaskClass::Auxiliary,
                            driver,
                            |driver| async move {
                                let _body = body;
                                tokio::select! {biased;_=cancel.cancelled()=>{},_=driver=>{}}
                                Ok(())
                            },
                        )
                        .map_err(std::io::Error::other)?;
                    *record.witness.lock().unwrap() = Some(witness);
                    record.registered.store(true, Ordering::Release);
                    drop(opening);
                    self.check(profile)?;
                    return Ok((connection, record));
                }
                Err(source) => {
                    drop(opening);
                    let unused_prepare = { prepare.lock().unwrap().is_some() };
                    let retry =
                        zero_platform_tokio::controlled_carrier::original_connect_error(&source)
                            .is_some()
                            && unused_prepare
                            && match &candidates {
                                Some(candidates) => candidates.has_next().await,
                                None => false,
                            };
                    if !retry {
                        return Err(std::io::Error::other(source).into());
                    }
                    self.settle_failed(&record, deadline)
                        .await
                        .map_err(std::io::Error::other)?;
                    if !record.closed() {
                        return Err(std::io::Error::other(
                            failure(self.instance, Stage::Close, Reason::Capacity)
                                .with_source(source),
                        )
                        .into());
                    }
                }
            }
        }
    }
}
pub struct Operation(Arc<ManagedPool>);
impl Drop for Operation {
    fn drop(&mut self) {
        self.0.operations.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
