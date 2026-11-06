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
        if demand.binding != *self.upstream.binding() {
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
        let lease = self.budget.reserve(self.instance)?;
        let pool = self
            .options
            .pool
            .upgrade()
            .ok_or_else(|| error(self.instance, Stage::Prepare, Reason::Revoked))?;
        let cached = pool.cached.lock().unwrap().clone();
        if let Some(Cached::Owned(client)) =
            cached.filter(|client| matches!(client,Cached::Owned(client) if !client.closed()))
        {
            return client.open(self, &demand, lease).await;
        }
        let _guard = self
            .wait(&demand, async { Ok(pool.connecting.lock().await) })
            .await?;
        if !self.ready() || pool.retired.load(Ordering::Acquire) {
            return Err(error(self.instance, Stage::Prepare, Reason::Revoked));
        }
        let cached = pool.cached.lock().unwrap().clone();
        if let Some(Cached::Owned(client)) =
            cached.filter(|client| matches!(client,Cached::Owned(client) if !client.closed()))
        {
            return client.open(self, &demand, lease).await;
        }
        // No request deadline means no invented setup deadline. For an actual
        // done+joined prior original, attempt only immediately ready cleanup;
        // unfinished originals retain capacity and their original controller.
        let cleanup_end = demand.deadline.unwrap_or_else(Instant::now);
        self.wait(&demand, self.reap(cleanup_end)).await?;
        let client = self.create(&demand, waiting).await?;
        if !self.ready() || pool.retired.load(Ordering::Acquire) {
            return Err(error(self.instance, Stage::Prepare, Reason::Revoked));
        }
        *pool.cached.lock().unwrap() = Some(Cached::Owned(client.clone()));
        client.open(self, &demand, lease).await
    }
    async fn create(
        self: &Arc<Self>,
        demand: &Demand,
        waiting: Option<zero_platform_tokio::contract::network::PendingCarrierObserver>,
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
        let mut grant = self.grants.reserve()?;
        let view = grant.claim_driver_view()?;
        let physical = Arc::new(Physical {
            adoption: grant.adoption(),
            stop: grant.cancellation(),
            view: Mutex::new(Some(view)),
            witness: Mutex::new(None),
            received: Mutex::new(None),
            accepted: AtomicBool::new(false),
            done: AtomicBool::new(false),
        });
        self.records.lock().unwrap().push(physical.clone());
        let body = Body(physical.clone());
        // The receiver and exact native cancellation are installed before the
        // actual caller or Node provider can await resolution/connect/TLS/H2.
        if let Some(waiting) = waiting.or_else(|| self.upstream.observed_pending_carrier()) {
            waiting.retain(physical.adoption.clone())?;
        }
        let acquiring = self.upstream.acquire_controlled_stream(
            self.options.endpoint.clone(),
            CarrierContext {
                session: None,
                deadline: demand.deadline,
                cancel: physical.stop.clone(),
            },
            grant,
        );
        let io = tokio::select! { biased;
            _=physical.stop.cancelled()=>return Err(demand_error(self.instance,Reason::Cancelled)),
            result=self.wait(demand, acquiring)=>result?,
        };
        let preparing = io.prepare_driver(|io| async {
            let io = zero_platform_tokio::TcpRelayStream::new(super::carrier::Exclusive::new(io));
            #[cfg(feature = "tls")]
            let io = match &self.options.settings.tls {
                Some(tls) => {
                    crate::tls::connect_tls_stream(io, tls, None, &self.options.settings.server)
                        .await?
                }
                None => io,
            };
            let settings = io
                .application_settings()
                .filter(|settings| settings.protocol == b"h2")
                .map(|settings| settings.peer.clone());
            crate::grpc::prepared::prepare_connection(
                io,
                self.options.settings.grpc.clone(),
                settings.as_deref(),
            )
            .await
        });
        let preparing = self.wait(demand, async {
            preparing.await.map_err(|source| {
                error(self.instance, Stage::Handshake, Reason::Io).with_source(source)
            })
        });
        let prepared = tokio::select! { biased;
            _=physical.stop.cancelled()=>return Err(demand_error(self.instance,Reason::Cancelled)),
            result=preparing=>result?,
        };
        let (client, driver) = prepared.split(|connection| {
            NativeClient::prepare(
                connection,
                physical.clone(),
                self.options.stream_limit.get(),
                &physical.stop,
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
