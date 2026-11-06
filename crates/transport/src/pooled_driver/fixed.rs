//! The existing fixed-library inline preparation contract, without new bounds.
use super::{
    failure,
    physical::{Body, Opening},
    Driver, ManagedPool, Physical,
};
use std::{
    future::Future,
    sync::{atomic::Ordering, Arc},
};
use tokio::time::Instant;
use zero_platform_tokio::{
    contract::{network::CarrierContext, scope::TaskClass, ChannelError, Reason, Stage},
    controlled_carrier::CarrierIo,
};
impl ManagedPool {
    async fn reap_fixed(&self, deadline: Instant) -> Result<(), ChannelError> {
        let records = self.records.lock().unwrap().clone();
        for record in records {
            record.receive_available();
            if !record.opening.load(Ordering::Acquire) && record.done.load(Ordering::Acquire) {
                self.settle_failed(&record, deadline).await?;
                if !record.closed() {
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
        self.reap_fixed(deadline)
            .await
            .map_err(std::io::Error::other)?;
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
