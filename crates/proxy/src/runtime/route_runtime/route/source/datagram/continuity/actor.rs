//! The actual listener task retains dispatch between original carrier attachments.
use super::*;
use crate::runtime::inbound_owners::PeerResourceContext;
use zero_platform_tokio::{contract::lifecycle::CloseReport, AcceptedPeerScope};
pub(super) async fn start(
    host: Arc<Host>,
    association: Arc<Association>,
    binding: Arc<HostPeerBinding>,
    resources: PeerResourceContext,
    route: InboundRouteRuntime,
    mut incoming: mpsc::Receiver<Attachment>,
    retention: Duration,
) -> Result<(), EngineError> {
    let stopped = binding.cancellation();
    let task = binding.wait_bound().await.map_err(std::io::Error::other)?;
    let witness = binding
        .join_witness()
        .ok_or_else(|| std::io::Error::other("association original host witness is absent"))?;
    let local = route
        .tcp_runtime
        .local_addr()
        .ok_or_else(|| std::io::Error::other("association requires actual listener endpoint"))?;
    let graph = AcceptedPeerScope::for_association(
        &host.tasks.peer_registrar(),
        task,
        witness,
        stopped.clone(),
        host.max_tasks,
        host.max_resources,
        local,
    )
    .map_err(std::io::Error::other)?;
    let tasks = graph.tasks();
    let state = Arc::new(AsyncMutex::new(super::super::owner::State::default()));
    let returns = Arc::new(crate::component::datagram_retirement::DatagramReturns::default());
    let retired = route
        .tcp_runtime
        .runtime_services()
        .execution()
        .retired_datagrams();
    let owner = owner::Owner {
        graph: graph.clone(),
        dispatch: super::super::owner::Owner {
            state: state.clone(),
            cancel: stopped.clone(),
            returns: returns.clone(),
            retired,
            peer_retirement: None,
        },
        host: host.clone(),
        association: association.clone(),
        receipts: CloseReport::default(),
    };
    resources
        .claim(tokio::time::Instant::now() + Duration::from_secs(5), |_| {
            binding.install_scope(graph)?;
            Ok((erase_lifecycle(owner), ()))
        })
        .await
        .map_err(std::io::Error::other)?;
    // Replace the original physical caller before any dispatcher/native I/O.
    let route = route
        .with_accepted_tasks(tasks.clone())
        .map_err(std::io::Error::other)?;
    let udp = route
        .udp_runtime()
        .with_datagram_returns(returns)
        .with_accepted_caller(tasks.caller().map_err(std::io::Error::other)?);
    let _closing = Closing(association.clone());
    let mut state = state.lock().await;
    let dispatch = udp.new_dispatch(route.inbound_tag()).await?;
    state.dispatch = Some(dispatch);
    let mut detached = tokio::time::Instant::now() + retention;
    let mut cancellations = tokio::time::interval(Duration::from_millis(100));
    cancellations.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let attachment = tokio::select! {biased;
            _=stopped.cancelled()=>return Ok(()),
            _=tokio::time::sleep_until(detached)=>return Ok(()),
            next=incoming.recv()=>match next {Some(next)=>next,None=>return Ok(())},
            _=cancellations.tick()=>{if state.dispatch.as_mut().unwrap().finish_pending_cancellations(){return Ok(());}continue;},
        };
        let Attachment {
            tx,
            rx,
            limits,
            negotiation,
            state: control,
            continuity,
            auth,
            source,
            cancel,
            done,
        } = attachment;
        let mut control = control.lock().await;
        let attached_udp = udp.with_source_addr(source);
        let result = tokio::select! {biased;
            _=stopped.cancelled()=>return Ok(()),
            result=super::super::relay::run(&attached_udp,route.inbound_tag(),state.dispatch.as_mut().unwrap(),control.control.as_mut().unwrap().as_mut(),tx,rx,limits,negotiation,auth.as_ref(),&cancel)=>result,
        };
        let terminal = matches!(&result, Ok(super::super::relay::Exit::AssociationEnded))
            || continuity.termination.reason() == zero_core::InboundMuxUdpTermination::ExplicitEnd
            || state
                .dispatch
                .as_mut()
                .unwrap()
                .finish_pending_cancellations();
        // TX/RX have returned. Consume the original protocol control outcome
        // before admitting a replacement transport; cancellation is not proof.
        let original = control.control.as_mut().unwrap();
        original.cancel(CloseReason::Shutdown);
        let report = original
            .wait_closed(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .map_err(std::io::Error::other)?;
        let complete = report.is_complete() && report.failed_tasks.is_empty();
        control.receipts.append(report);
        if !complete {
            return Err(std::io::Error::other("old continuity control remains unconfirmed").into());
        }
        control.control = None;
        drop(control);
        let generation = association.generation.load(Ordering::Acquire);
        if terminal {
            association.closing.store(true, Ordering::Release);
            let _ = done.send(result.map(|_| ()));
            return Ok(());
        }
        detached = tokio::time::Instant::now() + retention;
        if host
            .registry
            .detach(
                &association.scope,
                generation,
                retention,
                Some(association.clone()),
            )
            .is_err()
        {
            return Err(
                std::io::Error::other("association detach lost its original generation").into(),
            );
        }
        let _ = done.send(result.map(|_| ()));
    }
}
struct Closing(Arc<Association>);
impl Drop for Closing {
    fn drop(&mut self) {
        self.0.closing.store(true, Ordering::Release);
    }
}
