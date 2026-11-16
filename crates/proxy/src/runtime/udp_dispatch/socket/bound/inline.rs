//! Same installed dispatcher original and actual caller; no local Scope or spawn.
use super::*;
use crate::component::bound_datagram::{
    epochs::{Authorized, Epochs},
    HostBoundDatagramResources,
};
use crate::runtime::udp_socket::endpoint::peer::DirectBoundPeerOptions;
use zero_platform_tokio::contract::{
    component::ResourceDeclaration,
    lifecycle::{CloseReport, ResourceLifecycle},
    scope::TaskJoinWitness,
    CancellationToken, CloseReason, DeliveryState, Impact, Reason, Stage,
};

pub(super) struct Original {
    provider: Arc<HostBoundDatagramResources>,
    endpoint: DirectEndpointOwner,
    epochs: Epochs,
    caller: TaskJoinWitness,
    cancel: CancellationToken,
    report: CloseReport,
}
impl Original {
    pub(super) async fn start(&mut self) -> Result<(), ChannelError> {
        self.endpoint
            .start(tokio::time::Instant::now() + std::time::Duration::from_secs(5))
            .await
    }
    pub(super) fn cancellation(&self) -> CancellationToken {
        self.cancel.child_token()
    }
    pub(super) fn authorize(
        &self,
        source: crate::runtime::udp_dispatch::DirectSocketSource,
        generation: NetworkGeneration,
    ) -> Result<Authorized, ChannelError> {
        if !source.belongs_to_task(&self.caller) {
            return Err(error("Direct source belongs to another original caller"));
        }
        self.epochs.authorize(source, generation)
    }
    pub(super) async fn close(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.cancel.cancel();
        self.provider.revoke();
        self.endpoint.revoke(CloseReason::Shutdown);
        // The same original body/registration must return before native closure.
        // Waiting on the driver future or observing cancellation is insufficient.
        tokio::time::timeout_at(deadline, self.caller.wait())
            .await
            .map_err(|source| {
                error("actual Direct caller join remains pending").with_source(source)
            })?;
        let mut unfinished = Vec::new();
        let mut failure = None;
        match self.endpoint.shutdown(deadline).await {
            Ok(mut report) => {
                unfinished.append(&mut report.unfinished);
                self.report.append(report);
            }
            Err(source) => failure = Some(source),
        }
        match self.provider.shutdown_io() {
            Ok(mut report) => {
                unfinished.append(&mut report.unfinished);
                self.report.append(report);
            }
            Err(source) => {
                self.report.append(self.provider.take_closed_io());
                if failure.is_none() {
                    failure = Some(source);
                }
            }
        }
        if let Some(source) = failure {
            return Err(source);
        }
        let mut report = std::mem::take(&mut self.report);
        report.unfinished = unfinished;
        Ok(report)
    }
}
pub(super) fn build(
    runtime: &UdpIngressRuntime,
    budget: BoundDirectBudget,
) -> Result<BoundDirectSockets, ChannelError> {
    let execution = runtime.services().packet_path_execution().tcp();
    let current = execution
        .current_source
        .upgrade()
        .ok_or_else(|| error("actual Direct host retired"))?;
    let protocols = current
        .protocols()
        .map_err(|source| error("actual Direct registry retired").with_source(source))?;
    let caller = match runtime.peer_resources() {
        Some(context) => context.binding().join_witness(),
        None => runtime
            .accepted_caller()
            .and_then(|caller| caller.task_join_witness()),
    }
    .filter(|caller| !caller.is_joined())
    .ok_or_else(|| error("Direct has no actual original caller"))?;
    let task = caller
        .task_id()
        .ok_or_else(|| error("actual Direct task identity absent"))?;
    let resource = protocols
        .allocate_resource_identity()
        .map_err(|source| error("actual Direct identity exhausted").with_source(source))?;
    let dispatcher = protocols.allocate_peer_association()?;
    let identity = DirectEndpointIdentity {
        resource,
        ingress: runtime
            .peer_ingress()
            .map_err(|source| error("actual Direct ingress missing").with_source(source))?,
        dispatcher,
    };
    let cancel = runtime
        .inline_direct_cancellation()
        .ok_or_else(|| error("actual Direct caller cancellation missing"))?;
    let provider = HostBoundDatagramResources::new_for_peer_with_totals(
        resource,
        &caller,
        &ResourceDeclaration::BoundOutboundDatagram {
            max_sockets: budget.limits.native_sockets,
            max_bindings: budget.max_bindings,
        },
        cancel.clone(),
        protocols.runtime_host().original_budgets().carrier.clone(),
    )?;
    // These are native sockets, not a Packet-to-Flow client stack. Keep that
    // stack-only limit on its existing real stack rather than reinterpret it.
    let mut queue = execution.config().runtime.network.udp_receive_queue.clone();
    queue.max_stack_payload_bytes = None;
    let options = DirectBoundPeerOptions {
        identity,
        task,
        queue,
        limits: budget.limits,
    };
    let observer = runtime.services().network().direct_datagram_observer();
    let (endpoint, driver) = match runtime.peer_resources() {
        Some(context) => DirectEndpointOwner::for_bound_peer(options, context.binding(), observer)?,
        None => DirectEndpointOwner::for_bound_accepted(
            options,
            runtime
                .accepted_caller()
                .ok_or_else(|| error("accepted Direct caller missing"))?
                .clone(),
            cancel.clone(),
            observer,
        )?,
    };
    let bridge = DirectDispatchEndpoint::new(endpoint.endpoint(), execution.retired_datagrams());
    let epochs = Epochs::new(provider.clone(), budget.max_bindings);
    provider.activate(task)?;
    let resources: Arc<dyn ResourceAccess> = provider.clone();
    let original = Original {
        provider,
        endpoint,
        epochs,
        caller,
        cancel,
        report: Default::default(),
    };
    // All native slots are cold. The driver cannot acquire until the returned
    // dispatcher has been installed and its first real operation starts it.
    runtime
        .direct_peer_driver()
        .ok_or_else(|| error("actual Direct inline driver absent"))?
        .submit(driver)?;
    Ok(BoundDirectSockets {
        bridge,
        resources,
        authority: Authority::Inline(Box::new(original)),
        isolation: HashMap::new(),
        generation: 0,
        pre_io_bind_attempts: budget.pre_io_bind_attempts,
    })
}
fn error(message: &'static str) -> ChannelError {
    let mut source = ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Session)
        .with_source(std::io::Error::other(message));
    source.delivery = DeliveryState::NotSubmitted;
    source
}

#[cfg(test)]
impl Original {
    pub(super) fn original_views_for_test(
        &self,
    ) -> Vec<(
        zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin,
        zero_platform_tokio::contract::component::BoundDatagramView,
    )> {
        self.provider.original_views_for_test()
    }
}
