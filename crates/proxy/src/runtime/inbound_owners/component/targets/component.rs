//! Actual Source service driver, same retained native graph and Scope.
use super::*;
use zero_platform_tokio::contract::{
    lifecycle::{ResourceFailure, ResourceFailurePhase, Unfinished},
    scope::{TaskJoinWitness, WeakTaskRegistrar},
};
pub(in super::super) struct ComponentTargets {
    target: Targets,
    tasks: WeakTaskRegistrar,
    originals: Mutex<Vec<Arc<InboundTargetNativeOwner>>>,
    receipts: Mutex<CloseReport>,
    instance: zero_core::component::ComponentInstanceId,
    failure: Mutex<Option<Arc<ChannelError>>>,
    resource_failure: Mutex<Option<Arc<ResourceFailure>>>,
}
impl ComponentTargets {
    pub(in super::super) fn new(
        limit: std::num::NonZeroUsize,
        upstream: UpstreamConnectServices,
        instance: zero_core::component::ComponentInstanceId,
        tasks: &TaskRegistrar,
        root: &Arc<zero_platform_tokio::controlled_carrier::CarrierOriginalTotals>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            target: Targets {
                pool: InboundTargetSocketPool::new_component(
                    instance,
                    tasks.retirement_observation(),
                    root,
                    limit,
                    cancel,
                ),
                upstream,
            },
            tasks: tasks.downgrade(),
            originals: Mutex::new(Vec::new()),
            receipts: Mutex::new(CloseReport::default()),
            instance,
            failure: Mutex::new(None),
            resource_failure: Mutex::new(None),
        }
    }
    pub(in super::super) async fn open(
        &self,
        driver: &TaskJoinWitness,
        tasks: &WeakTaskRegistrar,
        endpoint: FallbackEndpoint,
    ) -> Result<TcpRelayStream, BuildError> {
        if let Some(failure) = self.failure.lock().unwrap().as_ref() {
            return Err(ChannelError::retained(failure.clone()));
        }
        if !tasks.registration_open()
            || !tasks
                .scope_identity()
                .same_scope(&self.tasks.scope_identity())
        {
            return Err(denied());
        }
        self.reap().await?;
        let native = self
            .target
            .pool
            .reserve_component_graph(driver, &endpoint)?;
        self.originals.lock().unwrap().push(native.clone());
        let stop = native.cancellation();
        let guard = stop.clone().drop_guard();
        let (go, admitted) = tokio::sync::oneshot::channel();
        let (send, ready) = tokio::sync::oneshot::channel();
        let original = native.clone();
        let target = self.target.clone();
        native.register_component_actor(&self.tasks,async move {
            if admitted.await.is_err(){return Ok(());}
            let connected=tokio::select!{biased;_=stop.cancelled()=>Err(std::io::Error::new(std::io::ErrorKind::Interrupted,"component target stopped")),result=target.connect(endpoint,original.clone())=>result};
            match connected {
                Ok(stream)=>match original.wrap_stream(stream) {
                    Ok(stream)=>{if send.send(Ok(stream)).is_ok(){original.wait_view_returned().await;}},
                    Err(source)=>{let _=send.send(Err(std::io::Error::other(source)));},
                },
                Err(source)=>{let _=send.send(Err(source));},
            }
            Ok(())
        })?;
        go.send(()).map_err(|_| denied())?;
        match ready.await.map_err(|e| denied().with_source(e))? {
            Ok(stream) => {
                guard.disarm();
                Ok(stream)
            }
            Err(source) => Err(
                ChannelError::new(Stage::Dial, Reason::Io, Impact::Component).with_source(source),
            ),
        }
    }
    async fn reap(&self) -> Result<(), BuildError> {
        let retired = self
            .originals
            .lock()
            .unwrap()
            .iter()
            .filter(|native| native.cancellation().is_cancelled() || native.is_closed())
            .cloned()
            .collect::<Vec<_>>();
        for original in retired {
            let report = original
                .close(Instant::now() + std::time::Duration::from_secs(5))
                .await
                .map_err(|source| self.remember(source))?;
            if !report.is_complete() {
                return Err(ChannelError::new(
                    Stage::Close,
                    Reason::Timeout,
                    Impact::Component,
                ));
            }
            // Consume the real prior controller/driver result before another
            // redirect/refresh can spend its finite native capacity.
            self.originals
                .lock()
                .unwrap()
                .retain(|native| !Arc::ptr_eq(native, &original));
            let mut receipts = self.receipts.lock().unwrap();
            if !report.closed_io.is_empty() {
                receipts.closed_io.clear();
                receipts
                    .closed_io
                    .push("original component target native receipts consumed".into());
            }
            if !report.released_resources.is_empty() {
                receipts.released_resources.clear();
                receipts
                    .released_resources
                    .push("original component target driver/view returns consumed".into());
            }
            receipts.unfinished.extend(report.unfinished);
        }
        Ok(())
    }
    pub(in super::super) fn revoke(&self) {
        let originals = self.originals.lock().unwrap().clone();
        for original in originals {
            original.cancellation().cancel();
        }
    }
    fn remember(&self, source: ChannelError) -> ChannelError {
        let source = source
            .retained_source()
            .cloned()
            .unwrap_or_else(|| Arc::new(source));
        let original = self.failure.lock().unwrap().get_or_insert(source).clone();
        ChannelError::retained(original)
    }
    pub(in super::super) async fn shutdown(&self, end: Instant) -> Result<CloseReport, BuildError> {
        self.revoke();
        let originals = self.originals.lock().unwrap().clone();
        let mut report = CloseReport::default();
        for original in originals {
            match original.close(end).await {
                Ok(receipt) => report.append(receipt),
                Err(source) => {
                    // Persist the same original fault before another await;
                    // continue closing other originals even after this error.
                    let _ = self.remember(source);
                    if !original.is_closed() {
                        report.unfinished.push(Unfinished::Resource(
                            "original component target close remains unconfirmed".into(),
                        ));
                    }
                }
            }
        }
        if let Some(source) = self.failure.lock().unwrap().clone() {
            let mut failure = self.resource_failure.lock().unwrap();
            let original = failure.get_or_insert_with(|| {
                Arc::new(ResourceFailure::new(
                    self.instance,
                    ResourceFailurePhase::DeclaredResources,
                    source,
                ))
            });
            report.resource_failures.push(original.clone());
        }
        report.append(std::mem::take(&mut *self.receipts.lock().unwrap()));
        Ok(report)
    }
}
