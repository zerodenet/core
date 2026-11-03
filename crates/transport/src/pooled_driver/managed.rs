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
        network::{CarrierContext, UpstreamTransport},
        scope::{TaskClass, TaskRegistrar},
        CancellationToken, ChannelError, Reason, Stage,
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
        self.operations
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| {
                std::io::Error::other(failure(self.instance, Stage::Prepare, Reason::Capacity))
            })?;
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
    pub(super) fn harvest(&self) {
        let records = self.records.lock().unwrap().clone();
        for record in records {
            record.receive_available();
        }
        self.records
            .lock()
            .unwrap()
            .retain(|record| !record.closed());
    }
    async fn reap(&self, deadline: Instant) -> Result<(), ChannelError> {
        let records = self.records.lock().unwrap().clone();
        for record in records {
            record.receive_available();
            if !record.opening.load(Ordering::Acquire) && record.done.load(Ordering::Acquire) {
                let report = record.original.retire(deadline).await?;
                let complete = report.is_complete();
                self.settled.lock().unwrap().append(report);
                if !complete || !record.closed() {
                    return Err(failure(self.instance, Stage::Close, Reason::Capacity));
                }
            }
        }
        self.harvest();
        Ok(())
    }
    /// The record and its actual original controller precede the first await.
    /// Preparation returns the same raw carrier's driver plus logical handles.
    pub async fn open<C, F, Fut>(
        self: &Arc<Self>,
        profile: &str,
        binding: &zero_platform_tokio::contract::network::UpstreamBinding,
        prepare: F,
    ) -> Result<(C, Arc<Physical>), crate::RuntimeError>
    where
        F: FnOnce(CarrierIo) -> Fut,
        Fut: Future<Output = Result<(C, Driver), crate::RuntimeError>>,
    {
        self.check(profile)?;
        let deadline = Instant::now()
            .checked_add(self.options.open_budget)
            .ok_or_else(|| std::io::Error::other("carrier deadline overflow"))?;
        self.reap(deadline).await.map_err(std::io::Error::other)?;
        if self.records.lock().unwrap().len() >= self.options.limit.get() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "live or retiring carrier pool is full",
            )
            .into());
        }
        let mut grant = self.grants.reserve().map_err(std::io::Error::other)?;
        let view = grant.claim_driver_view().map_err(std::io::Error::other)?;
        let record = Physical::new(grant.adoption(), self);
        {
            let mut records = self.records.lock().unwrap();
            records.push(record.clone());
        }
        let _opening = Opening(record.clone());
        let preparing = async {
            let selected = self
                .upstream
                .select_background_carrier(
                    Some(binding.clone()),
                    CarrierContext {
                        session: None,
                        deadline: Some(deadline),
                        cancel: record.stop.clone(),
                    },
                )
                .await
                .map_err(std::io::Error::other)?;
            if selected.binding() != binding {
                return Err(std::io::Error::other(failure(
                    self.instance,
                    Stage::Prepare,
                    Reason::StalePlan,
                ))
                .into());
            }
            self.verify_epoch(binding)?;
            let raw = if binding.prefix.is_empty() {
                let candidates = selected
                    .resolve_controlled_carrier(
                        self.options.endpoint.clone(),
                        CarrierContext {
                            session: None,
                            deadline: Some(deadline),
                            cancel: record.stop.clone(),
                        },
                    )
                    .await
                    .map_err(std::io::Error::other)?;
                if candidates.len() != 1 {
                    return Err(std::io::Error::other(failure(
                        self.instance,
                        Stage::Prepare,
                        Reason::StalePlan,
                    ))
                    .into());
                }
                candidates
                    .prepare_stream(0, grant)
                    .map_err(std::io::Error::other)?
                    .open(deadline)
                    .await
                    .map_err(std::io::Error::other)?
                    .into_parts()
                    .0
            } else {
                let call = CarrierContext {
                    session: None,
                    deadline: Some(deadline),
                    cancel: record.stop.clone(),
                };
                let loan = selected
                    .borrow_stream(self.options.endpoint.clone(), call.clone())
                    .await
                    .map_err(std::io::Error::other)?;
                grant
                    .adopt_stream_loan(binding, self.options.endpoint.clone(), &call, loan)
                    .map_err(std::io::Error::other)?
            };
            let tagged = raw.prepare_driver(prepare).await?;
            self.verify_epoch(binding)?;
            Ok::<_, crate::RuntimeError>(tagged)
        };
        let tagged = tokio::select! { biased;
            _ = self.cancel.cancelled() => return Err(std::io::Error::other(failure(self.instance, Stage::Dial, Reason::Revoked)).into()),
            result = tokio::time::timeout_at(deadline, preparing) => result.map_err(|source| std::io::Error::new(std::io::ErrorKind::TimedOut, source))??,
        };
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
                    let _view = view;
                    tokio::select! { biased; _=cancel.cancelled()=>{}, _=driver=>{} }
                    Ok(())
                },
            )
            .map_err(std::io::Error::other)?;
        *record.witness.lock().unwrap() = Some(witness);
        record.registered.store(true, Ordering::Release);
        self.check(profile)?;
        Ok((connection, record))
    }
}
pub struct Operation(Arc<ManagedPool>);
impl Drop for Operation {
    fn drop(&mut self) {
        self.0.operations.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
