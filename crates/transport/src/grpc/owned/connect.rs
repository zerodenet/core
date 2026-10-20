use super::{
    state::{Body, Physical},
    *,
};
use crate::grpc::pool::Cached;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use zero_platform_tokio::contract::{lifecycle::TaskClass, network::CarrierContext, DeliveryState};

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
            result=tokio::time::timeout_at(demand.deadline,operation)=>result.map_err(|source|demand_error(self.instance,Reason::Timeout).with_source(source))?,
        }
    }
    pub(super) async fn open(
        self: &Arc<Self>,
        demand: Demand,
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
        if Instant::now() >= demand.deadline {
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
        self.wait(&demand, self.reap(demand.deadline)).await?;
        let client = self.create(&demand).await?;
        if !self.ready() || pool.retired.load(Ordering::Acquire) {
            return Err(error(self.instance, Stage::Prepare, Reason::Revoked));
        }
        *pool.cached.lock().unwrap() = Some(Cached::Owned(client.clone()));
        client.open(self, &demand, lease).await
    }
    async fn create(self: &Arc<Self>, demand: &Demand) -> Result<Arc<NativeClient>, ChannelError> {
        if !self.ready() {
            return Err(error(self.instance, Stage::Prepare, Reason::Revoked));
        }
        if demand.cancel.is_cancelled() {
            return Err(demand_error(self.instance, Reason::Cancelled));
        }
        if Instant::now() >= demand.deadline {
            return Err(demand_error(self.instance, Reason::Timeout));
        }
        let grant = self.grants.reserve()?;
        let physical = Arc::new(Physical {
            adoption: grant.adoption(),
            witness: Mutex::new(None),
            done: AtomicBool::new(false),
        });
        self.records.lock().unwrap().push(physical.clone());
        let body = Body(physical.clone());
        let prepared = self.upstream.prepare_controlled_stream(
            self.options.endpoint.clone(),
            CarrierContext {
                session: None,
                deadline: Some(demand.deadline),
                cancel: self.cancel.clone(),
            },
            grant,
        )?;
        let ready = self.wait(demand, prepared.open(demand.deadline)).await?;
        let (io, _) = ready.into_parts();
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
        let prepared = self
            .wait(demand, async {
                preparing.await.map_err(|source| {
                    error(self.instance, Stage::Handshake, Reason::Io).with_source(source)
                })
            })
            .await?;
        let (client, driver) = prepared.split(|connection| {
            NativeClient::prepare(
                connection,
                physical.clone(),
                self.options.stream_limit.get(),
                &self.cancel,
            )
        });
        let stop = client.life.stop.clone();
        let scope_stop = self.scope_cancel.clone();
        let instance = self.instance;
        let (_,witness)=physical.adoption.spawn_driver(&self.tasks,TaskClass::Critical,driver,|driver|async move {
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
