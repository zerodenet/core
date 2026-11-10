//! Publish the exact Root original before setup; acquire only the original Source loan.
use super::factory::Shared;
use super::*;
use zero_platform_tokio::{
    contract::{
        context::{with_deadline, OpenContext},
        lifecycle::BoxChannelControl,
        network::CarrierContext,
        scope::TaskJoinWitness,
    },
    controlled_carrier::{
        loan::StreamLoan, CarrierAdoption, CarrierGrant, CarrierUserView, ProtocolOriginalChildren,
    },
};
pub(super) struct BeforeActor {
    pub entry: PacketMetadataArc<entry::Entry>,
    pub adoption: CarrierAdoption,
    pub attempted: bool,
    pub active: bool,
}
impl Drop for BeforeActor {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if !self.attempted {
            self.entry.set_actor(None);
            self.entry.skip_driver();
        } else if let Some(actual) = self.adoption.initializer_registration() {
            self.entry.set_actor(Some(actual));
        }
        if let Some(original) = self.entry.retirement.get() {
            original.request_retirement();
        }
    }
}
pub(super) struct Pending(pub Option<PacketMetadataArc<entry::Entry>>);
impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(entry) = self.0.as_ref() {
            entry.abandon_delivery();
        }
        if let Some(original) = self.0.as_ref().and_then(|entry| entry.retirement.get()) {
            original.request_retirement();
        }
    }
}
impl Shared {
    pub(super) async fn acquire_ws(
        shared: &PacketMetadataArc<Self>,
        context: &OpenContext,
    ) -> Result<StreamLoan, ChannelError> {
        if shared.cancel.is_cancelled() || context.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Cancelled));
        }
        if !context.upstream.binding().prefix.is_empty()
            || context.upstream.binding().dial != shared.options.protocol.dial
        {
            // This is the previously validated Browser grammar, before taking
            // a Source. Browser tasks never execute through a network prefix.
            return Err(error(Stage::Prepare, Reason::InvalidConfig));
        }
        let position = table::Table::reserve(&shared.entries)?;
        let header = shared
            .memory
            .reserve(PacketMetadataArc::<entry::Entry>::layout().map_err(capacity)?)
            .map_err(capacity)?;
        let diagnostics = diagnostics::Diagnostics::prepare(&shared.memory)?;
        let control_memory = shared
            .memory
            .reserve(PacketMetadataLayout::boxed::<entry::Control>())
            .map_err(capacity)?;
        let source = zero_transport::browser_dialer::PreparedBrowserLoan::prepare(
            shared.options.provider.dialer(shared.dependency)?,
            &shared.buffers,
            &shared.memory,
        )?;
        let (ready, mut result, result_weak) = delivery::prepare::<entry::Ready>(&shared.memory)?;
        let (begin, mut input, _) = delivery::prepare::<TaskJoinWitness>(&shared.memory)?;
        let original = PacketMetadataArc::from_prepaid(
            entry::Entry::new(
                diagnostics,
                shared.tasks.downgrade(),
                shared.cancel.child_token(),
                ProtocolOriginalChildren::new(shared.memory.clone(), 2),
                false,
                result_weak,
            ),
            header,
        )
        .map_err(|(source, _, _)| capacity(source))?;
        let control =
            BoxChannelControl::from_prepaid(entry::Control(original.clone()), control_memory)
                .map_err(|(source, _, _)| capacity(source))?;
        let (mut grant, retirement) = shared
            .grants
            .reserve_stream_protocol_original(&shared.tasks, control)
            .map_err(|rejected| {
                original.set_actor(None);
                original.skip_driver();
                rejected.error
            })?;
        let adoption = grant.adoption();
        assert!(original.retirement.set(retirement.clone()).is_ok());
        position.publish(original.clone());
        let mut before = BeforeActor {
            entry: original.clone(),
            adoption: adoption.clone(),
            attempted: false,
            active: true,
        };
        let call = CarrierContext {
            session: None,
            // The outer setup observer alone owns the first logical deadline.
            // This original Source remains bound to the real Entry fence.
            deadline: None,
            // This unique physical view belongs to the actual Entry. The
            // initializer separately observes its first logical caller below;
            // a later genuine cached keeper must survive that Query's exit.
            cancel: original.stop.clone(),
        };
        if let Some(observer) = context.upstream.protocol_original_observer() {
            let token = grant.protocol_stream_original(
                &context.upstream,
                observer.clone(),
                &shared.options.protocol.endpoint,
                &call,
                &shared.tasks,
                &retirement,
            )?;
            observer.retain_protocol_original(token)?;
        }
        let view = grant.claim_transferred_view()?;
        let binding = context.upstream.binding().clone();
        let worker_shared = shared.clone();
        let worker_entry = original.clone();
        let initializer_cancel = context.cancel.clone();
        before.attempted = true;
        let spawned = adoption.spawn_auxiliary(&shared.tasks, async move {
            let caller = input.receive().await?;
            let work = initialize_ws(
                &worker_shared,
                &worker_entry,
                (grant, view),
                source,
                &caller,
                &binding,
                &call,
            );
            let result = tokio::select! {biased;
                _=worker_entry.stop.cancelled()=>None,
                _=initializer_cancel.cancelled()=>None,
                result=work=>Some(result),
            };
            if let Some(result) = result {
                let ready_value = match result {
                    Ok(loan) => entry::Ready::Stream(loan),
                    Err(source) => entry::Ready::Failed(worker_entry.diagnostics.operation(source)),
                };
                // The actual caller owns this channel. Failure to receive
                // destroys IO here outside Root/Entry locks, while Root keeps
                // the same Source original and pure operation result.
                if let Err(returned) = ready.send(ready_value) {
                    drop(returned);
                    worker_entry.abandon_delivery();
                    if let Some(original) = worker_entry.retirement.get() {
                        original.request_retirement();
                    }
                }
            }
            // The result is durably held in the Root-installed same Entry.
            // An ordinary received setup rejection is not an unhandled task panic.
            Ok(())
        });
        let caller = match spawned {
            Ok((_, caller)) => caller,
            Err(source) => {
                original.set_actor(adoption.initializer_registration());
                original.skip_driver();
                before.active = false;
                retirement.request_retirement();
                return Err(source);
            }
        };
        original.set_actor(Some(caller.clone()));
        before.active = false;
        if begin.send(caller.clone()).is_err() {
            retirement.request_retirement();
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        let mut pending = Pending(Some(original.clone()));
        let work = async { result.receive().await };
        let ready = tokio::select! {biased;
            _=context.cancel.cancelled()=>return Err(error(Stage::Prepare,Reason::Cancelled)),
            _=shared.cancel.cancelled()=>return Err(error(Stage::Prepare,Reason::Revoked)),
            _=original.stop.cancelled()=>return Err(error(Stage::Prepare,Reason::Revoked)),
            ready=with_deadline(context.deadline,work)=>ready.map_err(|source|error(Stage::Prepare,Reason::Timeout).with_source(source))??,
        };
        if let entry::Ready::Failed(source) = ready {
            original.receive_delivery()?;
            return Err(ChannelError::retained(source));
        }
        let receive = async {
            original.receive_actor().await?;
            adoption.accept_auxiliary().await?;
            caller.wait_registration_released().await;
            Ok::<_, ChannelError>(())
        };
        tokio::select! {biased;
            _=context.cancel.cancelled()=>return Err(error(Stage::Prepare,Reason::Cancelled)),
            _=original.stop.cancelled()=>return Err(error(Stage::Prepare,Reason::Revoked)),
            result=with_deadline(context.deadline,receive)=>result.map_err(|source|error(Stage::Prepare,Reason::Timeout).with_source(source))??,
        }
        original.receive_delivery()?;
        match ready {
            entry::Ready::Stream(loan) => {
                drop(pending.0.take());
                Ok(loan)
            }
            entry::Ready::Failed(source) => Err(ChannelError::retained(source)),
            entry::Ready::Http(_) => Err(error(Stage::Prepare, Reason::Internal)),
        }
    }
}
async fn initialize_ws(
    shared: &PacketMetadataArc<Shared>,
    entry: &PacketMetadataArc<entry::Entry>,
    input: (CarrierGrant, CarrierUserView),
    prepared: zero_transport::browser_dialer::PreparedBrowserLoan,
    caller: &TaskJoinWitness,
    binding: &zero_platform_tokio::contract::network::UpstreamBinding,
    call: &CarrierContext,
) -> Result<StreamLoan, ChannelError> {
    let (grant, view) = input;
    let upstream = pending::prepare(shared, entry, caller.clone(), binding)?;
    let (profile, server, port, secure) = shared
        .options
        .plan
        .browser_websocket()
        .ok_or_else(|| error(Stage::Prepare, Reason::InvalidConfig))?;
    let loan = prepared
        .websocket_profile(profile, server, port, secure, &upstream, call)
        .await?;
    let source = loan
        .owner
        .original()
        .accepted()
        .cloned()
        .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
    assert!(entry.source.set(source.clone()).is_ok());
    let issuer = shared.resources.accepted_dependency_stream_loan_issuer(
        shared.dependency,
        &source,
        &shared.options.protocol.endpoint,
        binding,
        &grant,
    )?;
    let io = grant.adopt_dependency_stream_loan(
        binding,
        shared.options.protocol.endpoint.clone(),
        call,
        loan,
        issuer,
    )?;
    io.into_stream_loan(view)
}
