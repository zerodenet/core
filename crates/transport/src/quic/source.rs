//! Standard native source. Original Metadata provider + real existing Scope only.
use super::owned::*;
use std::sync::Arc;
use tokio::time::Instant;
use zero_platform_tokio::contract::{
    component::*, context::*, inbound::*, lifecycle::*, scope::TaskRegistrar, CancellationToken,
    ChannelError, CloseReason, Impact, Reason, Stage,
};
struct State {
    owner: Option<OwnedEndpointOwner>,
    handle: Option<OwnedEndpointHandle>,
    receiver: Option<DriverExitReceiver>,
    last_delivery: Option<DriverDelivery>,
    completion: Option<MetadataUsersCompletion>,
    unstarted: Option<
        zero_platform_tokio::controlled_datagram::metadata::users::UnstartedMetadataCompletion,
    >,
    rejected: Option<GrantedMetadataListener>,
}
pub struct Lifecycle {
    state: Arc<tokio::sync::Mutex<State>>,
    resources: Arc<dyn ResourceAccess>,
    tasks: TaskRegistrar,
    request: MetadataDatagramRequest,
    server: Option<quinn::ServerConfig>,
    limits: OwnedEndpointLimits,
    masks: Vec<crate::finalmask::udp::Mask>,
    cancel: CancellationToken,
}
pub struct Source<Make> {
    state: Arc<tokio::sync::Mutex<State>>,
    cancel: CancellationToken,
    make: Make,
    driver_limit: std::num::NonZeroUsize,
}
#[expect(
    clippy::too_many_arguments,
    reason = "existing concrete QUIC source keeps its original resource, task and credential inputs explicit"
)]
pub fn source<H: InboundHandshake, Make: Fn(OwnedIncoming) -> H + Send + 'static>(
    request: MetadataDatagramRequest,
    server: quinn::ServerConfig,
    masks: Vec<crate::finalmask::udp::Mask>,
    limits: OwnedEndpointLimits,
    resources: Arc<dyn ResourceAccess>,
    tasks: TaskRegistrar,
    cancel: CancellationToken,
    make: Make,
) -> (Lifecycle, Source<Make>) {
    let state = Arc::new(tokio::sync::Mutex::new(State {
        owner: None,
        handle: None,
        receiver: None,
        last_delivery: None,
        completion: None,
        unstarted: None,
        rejected: None,
    }));
    let cancel = cancel.child_token();
    (
        Lifecycle {
            state: state.clone(),
            resources,
            tasks,
            request,
            server: Some(server),
            limits,
            masks,
            cancel: cancel.clone(),
        },
        Source {
            state,
            cancel,
            make,
            driver_limit: limits.drivers,
        },
    )
}
impl ResourceLifecycle for Lifecycle {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
        crate::finalmask::owned::validate(&self.masks)
            .map_err(|e| failure(Reason::Unsupported).with_source(e))?;
        let mut state = self.state.lock().await;
        if self.cancel.is_cancelled() || state.owner.is_some() {
            return Err(failure(Reason::Revoked));
        }
        let mut grant = self
            .resources
            .listen_metadata_datagram(self.request)
            .await?;
        let Some(claim) = grant.users_claim.take() else {
            state.rejected = Some(grant);
            return Err(failure(Reason::Unsupported));
        };
        let socket = match HostedMetadataSocket::new(grant, &self.tasks) {
            Ok(socket) => socket,
            Err(rejected) => {
                let mut grant = rejected.grant;
                grant.users_claim = Some(claim);
                state.return_unstarted(grant)?;
                return Err(failure(Reason::InvalidConfig).with_source(rejected.error));
            }
        };
        let (owner, receiver) = match OwnedEndpointOwner::new_hosted_claimed(
            socket,
            self.tasks.clone(),
            self.limits,
            claim,
        ) {
            Ok(original) => original,
            Err(rejected) => {
                state.return_unstarted(rejected.native.into_unstarted(rejected.claim))?;
                return Err(failure(Reason::Revoked));
            }
        };
        state.completion = owner.users_completion();
        state.receiver = Some(receiver);
        state.owner = Some(owner);
        state
            .owner
            .as_mut()
            .unwrap()
            .start_masks(&self.masks, self.resources.as_ref())?;
        // Persistent protocol owner is installed before any actual Quinn spawn.
        let handle =
            state
                .owner
                .as_mut()
                .unwrap()
                .start(Default::default(), self.server.take(), None)?;
        state.handle = Some(handle);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.cancel.cancel();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.cancel.cancel();
        let mut state = tokio::time::timeout_at(deadline, self.state.lock())
            .await
            .map_err(|e| failure(Reason::Timeout).with_source(e))?;
        state.handle = None;
        if let Some(owner) = state.owner.as_ref() {
            owner.begin_shutdown();
        }
        if state.owner.is_some() {
            // Host joins the same real drivers concurrently. Reception consumes
            // original witnesses and never substitutes a body result for a join.
            let ids = state.owner.as_ref().unwrap().shutdown_driver_tasks();
            for id in ids {
                state
                    .owner
                    .as_ref()
                    .unwrap()
                    .stop_mask_driver(id, deadline)
                    .await?;
                let delivery = state
                    .receiver
                    .as_mut()
                    .unwrap()
                    .receive(id, deadline)
                    .await?;
                state.last_delivery = Some(delivery);
                let State {
                    receiver,
                    last_delivery,
                    ..
                } = &mut *state;
                let delivery = last_delivery.as_ref().unwrap();
                receiver.as_mut().unwrap().release_body(delivery)?;
                report_delivery(delivery);
            }
            let report = state
                .owner
                .as_ref()
                .unwrap()
                .shutdown_users(deadline)
                .await?;
            if !report.users_and_drivers_complete() {
                return Err(failure(Reason::Timeout));
            }
            state.owner = None;
            state.receiver = None;
        }
        if let Some(completion) = state.completion.as_ref() {
            let receipt = completion
                .try_owned_closed()
                .ok_or_else(|| failure(Reason::Timeout))?;
            self.resources
                .confirm_metadata_users(self.request.ordinal, &receipt, &self.tasks)?;
            state.completion = None;
        }
        if state.rejected.is_some() {
            return Err(failure(Reason::InvalidConfig));
        }
        if let Some(unstarted) = state.unstarted.as_ref() {
            let receipt = unstarted
                .try_closed()
                .ok_or_else(|| failure(Reason::Timeout))?;
            self.resources
                .confirm_metadata_users(self.request.ordinal, &receipt, &self.tasks)?;
            state.unstarted = None;
        }
        Ok(CloseReport::default())
    }
}
impl<H: InboundHandshake, Make: Fn(OwnedIncoming) -> H + Send + 'static> InboundSource
    for Source<Make>
{
    type Handshake = PendingInbound;
    async fn next(
        &mut self,
        context: AcceptContext,
    ) -> Result<Option<PendingInbound>, ChannelError> {
        let mut state = self.state.lock().await;
        let handle = state
            .handle
            .clone()
            .ok_or_else(|| failure(Reason::Revoked))?;
        loop {
            let admit =
                state.owner.as_ref().unwrap().driver_status().retained < self.driver_limit.get();
            let receiver = state
                .receiver
                .as_mut()
                .ok_or_else(|| failure(Reason::Revoked))?;
            tokio::select! {biased;
                _=self.cancel.cancelled()=>return Ok(None),
                _=context.cancel.cancelled()=>return Err(failure(Reason::Cancelled)),
                _=async {match context.deadline {Some(deadline)=>tokio::time::sleep_until(deadline).await,None=>std::future::pending().await}}=>return Err(failure(Reason::Timeout)),
                delivery=receiver.receive_next()=>{
                    state.last_delivery=Some(delivery?);
                    let mask_failure = {
                        let State {receiver,last_delivery,..}=&mut *state;
                        let delivery=last_delivery.as_ref().unwrap();
                        receiver.as_mut().unwrap().release_body(delivery)?;
                        report_delivery(delivery);
                        delivery.body.as_ref().is_some_and(|body| matches!(body.origin, OwnedDriverOrigin::Mask {..})).then(|| match delivery.joined.result() {
                            Ok(())=>failure(Reason::Io),
                            Err(source)=>failure(Reason::Io).with_source(source.clone()),
                        })
                    };
                    if let Some(source) = mask_failure {
                        state.owner.as_ref().unwrap().begin_shutdown();
                        return Err(source);
                    }
                    state.owner.as_ref().unwrap().reclaim_received_drivers();
                },
                incoming=with_deadline(context.deadline,handle.accept_incoming_wait()),if admit=>{
                    let incoming=incoming.map_err(|e|failure(Reason::Timeout).with_source(e))??;
                    let facts=InboundConnectionFacts{original_source:Some(incoming.remote_address()),effective_source:Some(incoming.remote_address()),local_endpoint:Some(incoming.local_addr()),sni:None};
                    return Ok(Some(PendingInbound::new((self.make)(incoming)).with_carrier_facts(facts)));
                }
            }
        }
    }
}
fn failure(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Component)
}
fn report_delivery(delivery: &DriverDelivery) {
    if let Err(source) = delivery.joined.result() {
        tracing::debug!(?source, "original carrier driver received");
    }
}

/// Bounded actual carrier stream delivery. Drop only requests stop; the actual
/// accepted Scope joins every driver and original Metadata users prove return.
pub struct Streams {
    pub(crate) receiver:
        tokio::sync::Mutex<tokio::sync::mpsc::Receiver<zero_platform_tokio::TcpRelayStream>>,
    pub(crate) cancel: CancellationToken,
}
impl Streams {
    pub async fn accept(&self) -> Option<zero_platform_tokio::TcpRelayStream> {
        self.receiver.lock().await.recv().await
    }
    pub fn close(&self) {
        self.cancel.cancel();
    }
}
impl Drop for Streams {
    fn drop(&mut self) {
        self.close();
    }
}

impl State {
    fn return_unstarted(&mut self, grant: GrantedMetadataListener) -> Result<(), ChannelError> {
        match grant.return_unstarted() {
            Ok(completion) => {
                self.unstarted = Some(completion);
                Ok(())
            }
            Err(grant) => {
                self.rejected = Some(grant);
                Err(failure(Reason::InvalidConfig))
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/quic/source_deadline.rs"]
mod tests;
