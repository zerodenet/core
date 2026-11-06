use super::{
    state::{Body, Physical},
    *,
};
use crate::grpc::pool::Cached;
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use zero_platform_tokio::contract::{DeliveryState, lifecycle::TaskClass, network::CarrierContext};

impl Managed {
    pub(super) async fn wait<T>(
        &self,
        demand: &Demand,
        operation: impl Future<Output = Result<T, ChannelError>>,
    ) -> Result<T, ChannelError> {
        tokio::select! {biased;
            _=self.cancel.cancelled()=>Err(error(self.instance,Stage::Prepare,Reason::Revoked)),
            _=self.scope_cancel.cancelled()=>Err(error(self.instance,Stage::Prepare,Reason::Revoked)),
            _=demand.cancel.cancelled()=>Err(demand_error(self.instance,Reason::Cancelled)),
            result=zero_platform_tokio::contract::context::with_deadline(demand.deadline,operation)=>result.map_err(|source|demand_error(self.instance,Reason::Timeout).with_source(source))?,
        }
    }
    pub(super) async fn open(
        self: &Arc<Self>,
        demand: Demand,
        waiting: Option<zero_platform_tokio::contract::network::PendingCarrierObserver>,
    ) -> Result<NativeGrpcStream, ChannelError> {
        if (!self.options.selected_background && demand.binding != *self.upstream.binding())
            || demand.binding.dial != self.options.settings.dial
        {
            let mut error = error(self.instance, Stage::Prepare, Reason::StalePlan);
            error.delivery = DeliveryState::NotSubmitted;
            return Err(error);
        }
        if !self.ready() {
            return Err(error(self.instance, Stage::Prepare, Reason::Revoked));
        }
        if demand.cancel.is_cancelled() {
            return Err(demand_error(self.instance, Reason::Cancelled));
        }
        if demand
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            let mut error = demand_error(self.instance, Reason::Timeout);
            error.delivery = DeliveryState::NotSubmitted;
            return Err(error);
        }
        let selected = if self.options.selected_background {
            self.wait(
                &demand,
                self.upstream.select_background_carrier(
                    Some(demand.binding.clone()),
                    CarrierContext {
                        session: None,
                        deadline: demand.deadline,
                        cancel: demand.cancel.clone(),
                    },
                ),
            )
            .await?
        } else {
            self.upstream.clone()
        };
        if selected.binding() != &demand.binding {
            return Err(error(self.instance, Stage::Prepare, Reason::StalePlan));
        }
        if self.options.selected_background {
            self.admit_epoch(&demand, selected.carrier_dns_generation())
                .await?;
        }
        let totals = self.totals.lock().unwrap().clone();
        let lease = self.budget.reserve(self.instance, totals.as_ref())?;
        let pool = self
            .options
            .pool
            .upgrade()
            .ok_or_else(|| error(self.instance, Stage::Prepare, Reason::Revoked))?;
        let cached = pool.cached.lock().unwrap().clone();
        if let Some(Cached::Owned(clients)) = cached {
            if let Some(client) = clients
                .into_iter()
                .find(|client| client.binding == demand.binding && !client.closed())
            {
                return client.open(self, &demand, lease).await;
            }
        }
        let _guard = self
            .wait(&demand, async { Ok(pool.connecting.lock().await) })
            .await?;
        if !self.ready() || pool.retired.load(Ordering::Acquire) {
            return Err(error(self.instance, Stage::Prepare, Reason::Revoked));
        }
        let cached = pool.cached.lock().unwrap().clone();
        if let Some(Cached::Owned(clients)) = cached {
            if let Some(client) = clients
                .into_iter()
                .find(|client| client.binding == demand.binding && !client.closed())
            {
                return client.open(self, &demand, lease).await;
            }
        }
        self.wait(&demand, self.reap_demand(demand.deadline))
            .await?;
        let client = self.create(&demand, waiting).await?;
        if !self.ready() || pool.retired.load(Ordering::Acquire) {
            return Err(error(self.instance, Stage::Prepare, Reason::Revoked));
        }
        let previous = {
            let mut cache = pool.cached.lock().unwrap();
            if matches!(cache.as_ref(), Some(Cached::Legacy(_))) {
                // A conflicting actual legacy owner is retained unchanged;
                // error/Drop cannot certify its original controller closed.
                return Err(error(self.instance, Stage::Prepare, Reason::StalePlan));
            }
            cache.take()
        };
        let mut clients = match previous {
            Some(Cached::Owned(clients)) => clients,
            None => Vec::new(),
            Some(Cached::Legacy(_)) => {
                return Err(error(self.instance, Stage::Prepare, Reason::StalePlan));
            }
        };
        // Source/view destruction and callbacks run outside the cache lock.
        clients.retain(|client| !client.closed());
        clients.push(client.clone());
        {
            let mut cache = pool.cached.lock().unwrap();
            if !self.ready() || pool.retired.load(Ordering::Acquire) {
                drop(cache);
                drop(clients);
                return Err(error(self.instance, Stage::Prepare, Reason::Revoked));
            }
            *cache = Some(Cached::Owned(clients));
        }
        client.open(self, &demand, lease).await
    }
    pub(super) async fn create_attempt(
        self: &Arc<Self>,
        demand: &Demand,
        waiting: Option<zero_platform_tokio::contract::network::PendingCarrierObserver>,
        candidates: Option<Arc<super::candidates::Candidates>>,
        attempted: &mut Option<Arc<Physical>>,
    ) -> Result<Arc<NativeClient>, ChannelError> {
        if !self.ready() {
            return Err(error(self.instance, Stage::Prepare, Reason::Revoked));
        }
        if demand.cancel.is_cancelled() {
            return Err(demand_error(self.instance, Reason::Cancelled));
        }
        if demand
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(demand_error(self.instance, Reason::Timeout));
        }
        let totals = self.totals.lock().unwrap().clone();
        let total = totals
            .as_ref()
            .map(|totals| totals.physical(self.instance))
            .transpose()?;
        let mut grant = match self.grants.reserve() {
            Ok(grant) => grant,
            Err(source) => {
                if let Some(total) = &total {
                    total.refuse_unassigned()?;
                }
                return Err(source);
            }
        };
        let physical = Arc::new(Physical {
            pending: Mutex::new(Vec::new()),
            adoption: grant.adoption(),
            stop: grant.cancellation(),
            view: Mutex::new(None),
            witness: Mutex::new(None),
            received: Mutex::new(None),
            accepted: AtomicBool::new(false),
            done: AtomicBool::new(false),
        });
        self.records.lock().unwrap().push(physical.clone());
        *attempted = Some(physical.clone());
        if let Some(total) = total {
            total.install(physical.clone())?;
        }
        let body = Body(physical.clone());
        *physical.view.lock().unwrap() = Some(grant.claim_driver_view()?);
        // The receiver and exact native cancellation are installed before the
        // actual caller or Node provider can await resolution/connect/TLS/H2.
        let waiting = waiting.or_else(|| self.upstream.observed_pending_carrier());
        if let Some(waiting) = &waiting {
            waiting.retain(physical.adoption.clone())?;
        }
        let owner = self.clone();
        let preparing_physical = physical.clone();
        let operation_demand = Demand {
            deadline: demand.deadline,
            cancel: physical.stop.clone(),
            binding: demand.binding.clone(),
        };
        let operation = move |caller| {
            owner.prepare_original(
                preparing_physical,
                operation_demand,
                grant,
                waiting,
                caller,
                candidates,
            )
        };
        let prepared = self
            .wait(demand, async {
                if self.options.scoped_preparation {
                    super::setup::run(self, &physical, move |caller| operation(Some(caller))).await
                } else {
                    operation(None).await
                }
            })
            .await?;
        let (client, driver) = prepared.split(|connection| {
            NativeClient::prepare(
                connection,
                physical.clone(),
                self.options.stream_limit.get(),
                &physical.stop,
                demand.binding.clone(),
            )
        });
        let stop = client.life.stop.clone();
        let scope_stop = self.scope_cancel.clone();
        let instance = self.instance;
        let (_,witness)=physical.adoption.spawn_driver(&self.tasks,TaskClass::Auxiliary,driver,|driver|async move {
            let _body=body;
            tokio::select! {biased;
                _=stop.cancelled()=>Ok(()),
                _=scope_stop.cancelled()=>Ok(()),
                result=driver=>result.map_err(|source|error(instance,Stage::Receive,Reason::Io).with_source(source)),
            }
        })?;
        *physical.witness.lock().unwrap() = Some(witness);
        Ok(client)
    }
}
fn demand_error(instance: ComponentInstanceId, reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(Stage::Handshake, reason, Impact::Session);
    error.component = Some(instance.component);
    error
}
