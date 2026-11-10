//! One GET setup and one actual data driver for this logical Browser XHTTP call.
//! This preserves the original no-XMUX Browser grammar and creates no pool.
use super::*;
use super::{
    acquire::{BeforeActor, Pending},
    factory::Shared,
};
use zero_platform_tokio::{
    contract::{
        context::{with_deadline, OpenContext},
        lifecycle::{BoxChannelControl, TaskClass},
        network::{CarrierContext, UpstreamBinding},
        scope::TaskJoinWitness,
    },
    controlled_carrier::{
        loan::StreamLoan, CarrierDriver, CarrierIo, CarrierPreparation, CarrierUserView,
        ProtocolOriginalChildren,
    },
};
pub(super) struct Prepared {
    input: CarrierDriver<(CarrierIo, CarrierUserView)>,
    stream: zero_transport::split_http::XhttpStream,
    driver: zero_transport::split_http::BrowserXhttpDriver,
    preparation: CarrierPreparation,
    reservation: BeforeDriver,
}
struct BeforeDriver {
    entry: PacketMetadataArc<entry::Entry>,
    active: bool,
    attempted: bool,
}
impl Drop for BeforeDriver {
    fn drop(&mut self) {
        if self.active {
            if !self.attempted {
                self.entry.skip_driver();
            }
            if let Some(original) = self.entry.retirement.get() {
                original.request_retirement();
            }
        }
    }
}
impl Shared {
    pub(super) async fn acquire_http(
        shared: &PacketMetadataArc<Self>,
        context: &OpenContext,
    ) -> Result<StreamLoan, ChannelError> {
        if shared.cancel.is_cancelled() || context.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Cancelled));
        }
        if !context.upstream.binding().prefix.is_empty()
            || context.upstream.binding().dial != shared.options.protocol.dial
        {
            return Err(error(Stage::Prepare, Reason::InvalidConfig));
        }
        let position = table::Table::reserve(&shared.entries)?;
        let (profile, server, port, secure) = shared
            .options
            .plan
            .browser_http()
            .ok_or_else(|| error(Stage::Prepare, Reason::InvalidConfig))?;
        let authority_credit = shared
            .buffers
            .try_reserve(
                server
                    .len()
                    .checked_add(24)
                    .ok_or_else(|| error(Stage::Prepare, Reason::Capacity))?,
            )
            .map_err(capacity)?;
        let mut authority = String::with_capacity(server.len() + 24);
        use std::fmt::Write;
        if server.contains(':') && !server.starts_with('[') {
            write!(&mut authority, "[{server}]").unwrap();
        } else {
            authority.push_str(server);
        }
        if !(secure && port == 443 || !secure && port == 80) {
            write!(&mut authority, ":{port}").unwrap();
        }
        let prepared = zero_transport::split_http::PreparedBrowserXhttp::prepare(
            shared.options.provider.dialer(shared.dependency)?,
            profile,
            if secure { "https" } else { "http" },
            &authority,
            &shared.buffers,
            &shared.memory,
        )?;
        drop(authority);
        drop(authority_credit);
        let header = shared
            .memory
            .reserve(PacketMetadataArc::<entry::Entry>::layout().map_err(capacity)?)
            .map_err(capacity)?;
        let diagnostics = diagnostics::Diagnostics::prepare(&shared.memory)?;
        let control_memory = shared
            .memory
            .reserve(PacketMetadataLayout::boxed::<entry::Control>())
            .map_err(capacity)?;
        let logical_memory = shared
            .memory
            .reserve(PacketMetadataLayout::boxed::<super::roles::logical::Logical>())
            .map_err(capacity)?;
        let io_memory = shared
            .memory
            .reserve(PacketMetadataLayout::boxed::<
                zero_transport::split_http::XhttpStream,
            >())
            .map_err(capacity)?;
        let (ready_output, mut result, result_weak) =
            delivery::prepare::<entry::Ready>(&shared.memory)?;
        let (begin, mut input, _) = delivery::prepare::<TaskJoinWitness>(&shared.memory)?;
        let original = PacketMetadataArc::from_prepaid(
            entry::Entry::new(
                position.ordinal,
                diagnostics,
                shared.tasks.downgrade(),
                shared.cancel.child_token(),
                ProtocolOriginalChildren::new(shared.memory.clone(), 2),
                true,
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
        assert!(original.retirement.set(retirement.clone()).is_ok());
        position.publish(original.clone());
        let adoption = grant.adoption();
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
        let (view, preparation) = grant.claim_preparing_driver_view()?;
        let binding = context.upstream.binding().clone();
        let worker_entry = original.clone();
        let worker_shared = shared.clone();
        let worker_binding = binding.clone();
        let initializer_cancel = context.cancel.clone();
        before.attempted = true;
        let spawned=adoption.spawn_auxiliary(&shared.tasks,async move {
            let caller=input.receive().await?;
            let work=async {
                let upstream=pending::prepare(&worker_shared,&worker_entry,caller,&worker_binding)?;
                let ready=prepared.open_get(&upstream,&call).await?;
                let (loan,stream,driver)=ready.into_parts();
                let source=loan.owner.original().accepted().cloned().ok_or_else(||error(Stage::Prepare,Reason::StalePlan))?;
                assert!(worker_entry.source.set(source.clone()).is_ok());
                let issuer=worker_shared.resources.accepted_dependency_stream_loan_issuer(worker_shared.dependency,&source,&worker_shared.options.protocol.endpoint,&worker_binding,&grant)?;
                let io=grant.adopt_dependency_stream_loan(&worker_binding,worker_shared.options.protocol.endpoint.clone(),&call,loan,issuer)?;
                let input=io.into_driver().try_map(|io|Ok::<_,std::convert::Infallible>((io,view))).unwrap();
                Ok::<_,ChannelError>(Prepared{input,stream,driver,preparation,reservation:BeforeDriver{entry:worker_entry.clone(),active:true,attempted:false}})
            };
            let result=tokio::select!{biased;_ = initializer_cancel.cancelled()=>None,_=worker_entry.stop.cancelled()=>None,result=work=>Some(result)};
            let ready=match result {
                Some(Ok(prepared))=>Some(entry::Ready::Http(prepared)),
                Some(Err(source))=>{worker_entry.skip_driver();Some(entry::Ready::Failed(worker_entry.diagnostics.operation(source)))},
                None=>{worker_entry.skip_driver();None},
            };
            if let Some(value)=ready {
                if let Err(returned)=ready_output.send(value) {
                    drop(returned);worker_entry.skip_driver();worker_entry.abandon_delivery();
                    if let Some(original)=worker_entry.retirement.get(){original.request_retirement();}
                }
            }
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
        let mut pending_guard = Pending(Some(original.clone()));
        if begin.send(caller.clone()).is_err() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        let work = async {
            let ready = result.receive().await?;
            if let entry::Ready::Failed(source) = ready {
                original.receive_delivery()?;
                return Err(ChannelError::retained(source));
            }
            original.receive_actor().await?;
            adoption.accept_auxiliary().await?;
            let joined = caller.wait_registration_released().await;
            if let Err(source) = joined.result() {
                return Err(ChannelError::retained(source));
            }
            match ready {
                entry::Ready::Http(prepared) => {
                    original.receive_delivery()?;
                    start_driver(
                        shared,
                        &original,
                        adoption.clone(),
                        prepared,
                        &binding,
                        logical_memory,
                        io_memory,
                    )
                }
                _ => {
                    original.skip_driver();
                    Err(error(Stage::Prepare, Reason::Internal))
                }
            }
        };
        let loan = tokio::select! {biased;_=context.cancel.cancelled()=>return Err(error(Stage::Prepare,Reason::Cancelled)),_=shared.cancel.cancelled()=>return Err(error(Stage::Prepare,Reason::Revoked)),_=original.stop.cancelled()=>return Err(error(Stage::Prepare,Reason::Revoked)),result=with_deadline(context.deadline,work)=>result.map_err(|source|error(Stage::Prepare,Reason::Timeout).with_source(source))??};
        drop(pending_guard.0.take());
        Ok(loan)
    }
}
fn start_driver(
    shared: &PacketMetadataArc<Shared>,
    entry: &PacketMetadataArc<entry::Entry>,
    adoption: zero_platform_tokio::controlled_carrier::CarrierAdoption,
    prepared: Prepared,
    binding: &UpstreamBinding,
    logical_memory: zero_traits::packet_metadata::PacketMetadataCredit,
    io_memory: zero_traits::packet_metadata::PacketMetadataCredit,
) -> Result<StreamLoan, ChannelError> {
    if entry.stop.is_cancelled() {
        return Err(error(Stage::Prepare, Reason::Revoked));
    }
    let Prepared {
        input,
        stream,
        driver,
        preparation,
        reservation: mut guard,
    } = prepared;
    let retirement = entry.retirement.get().unwrap();
    // The actual input still identifies this same reservation; no lookup from
    // task numbers or an anonymous same-Scope driver's view is accepted.
    let (begin, mut start, _) = delivery::prepare::<TaskJoinWitness>(&shared.memory)?;
    let weak = PacketMetadataArc::downgrade(shared);
    let worker_entry = entry.clone();
    let binding = binding.clone();
    guard.attempted = true;
    let spawned = adoption.spawn_driver(
        &shared.tasks,
        TaskClass::Critical,
        input,
        move |(io, view)| async move {
            let caller = start.receive().await?;
            let shared = weak
                .upgrade()
                .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
            let upstream = pending::prepare(&shared, &worker_entry, caller, &binding)?;
            drop(shared);
            let call = CarrierContext {
                session: None,
                deadline: None,
                cancel: worker_entry.stop.clone(),
            };
            let _view = view;
            driver.run(io, upstream, call).await
        },
    );
    let caller = match spawned {
        Ok((_, caller)) => caller,
        Err(source) => {
            entry.skip_driver();
            guard.active = false;
            retirement.request_retirement();
            return Err(source);
        }
    };
    entry.set_driver(caller.clone());
    guard.active = false;
    preparation.publish_driver(&caller)?;
    let original = adoption.origin(&caller)?;
    let control = BoxChannelControl::from_prepaid(
        super::roles::logical::Logical::Driver(adoption),
        logical_memory,
    )
    .map_err(|(source, _, _)| capacity(source))?;
    let loan = StreamLoan::from_prepaid_io(stream, control, original, io_memory)
        .map_err(|(source, _, _, _, _)| capacity(source))?;
    // Driver's original witness was stored in both Slot and Entry before it can
    // poll even its first POST or publish another accepted Source child.
    begin
        .send(caller)
        .map_err(|_| error(Stage::Prepare, Reason::Revoked))?;
    Ok(loan)
}
