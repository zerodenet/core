//! Actual second UDP open and output over the same Source/Root after cleanup.
use super::*;
pub(super) async fn actual_retry(
    grant: zero_platform_tokio::contract::component::PacketDriverCallGrant,
    wire: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    users: &Arc<crate::runtime::packet_flow::FlowUsers>,
    stack: &ClientUdpStack,
    read: &zero_tun::packet_memory::NativeReadGrant,
    original: &CarrierOrigin,
    peer: SocketAddr,
    target: Destination,
    output: &mut tokio::sync::mpsc::Receiver<zero_traits::PacketBuffer>,
) {
    let custody = grant
        .pending_observer()
        .packet_driver_custody(&grant.identity(), &grant.call())
        .unwrap()
        .unwrap();
    let user = users
        .acquire_from_packet_call(wire.clone(), Some(custody))
        .unwrap();
    let receiver = user.receiver_binding();
    let socket = stack.bind("10.0.0.1".parse().unwrap()).unwrap();
    let session = session(
        socket,
        peer,
        target.clone(),
        AssociationId(2),
        CancellationToken::new(),
        user,
        users.cancellation().clone(),
    );
    let mut loan = DatagramLoan::new(
        zero_platform_tokio::contract::datagram::erase_session(session),
        original.clone(),
        AssociationId(2),
        target.clone(),
    );
    let observation = loan.receiver_observation();
    receiver.bind(observation, &mut loan.owner).unwrap();
    let (session, handoff) = grant.install_datagram_loan(loan, AssociationId(2)).unwrap();
    handoff.confirm_receiver(wire).unwrap();
    let zero_platform_tokio::contract::datagram::DatagramSession {
        mut tx,
        mut rx,
        mut control,
        limits,
    } = session;
    let mut payload = read
        .memory_view()
        .unwrap()
        .transmit_packet(b"recovered".len(), 0, 0)
        .unwrap();
    payload.copy_from_slice(b"recovered");
    let frame = zero_core::packet::Datagram {
        association: AssociationId(2),
        address: target,
        payload,
    };
    let sent = zero_platform_tokio::contract::datagram::send(
        tx.as_mut(),
        control.as_mut(),
        &frame,
        &limits,
    )
    .await
    .unwrap();
    assert_eq!(sent.accepted_bytes, b"recovered".len());
    let packet = output.recv().await.unwrap();
    assert_eq!(
        zero_stack::packet::parse_udp(&packet).unwrap().payload,
        b"recovered"
    );
    let sent = zero_stack::packet::parse_udp(&packet).unwrap();
    let reply =
        zero_stack::packet::build_udp(peer.ip(), sent.src.ip, peer.port(), sent.src.port, b"reply");
    drop(packet);
    let mut native = read.try_buffer().unwrap();
    native.as_mut_bytes()[..reply.len()].copy_from_slice(&reply);
    let original_reply = native
        .into_packet(reply.len())
        .unwrap_or_else(|_| panic!("initialized packet length fits the original read workspace"));
    let original_payload = original_reply[28..].as_ptr();
    stack.feed_correlated_packet(original_reply).unwrap();
    let received = poll_fn(|cx| rx.as_mut().poll_recv(cx))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.payload.as_ref(), b"reply");
    assert_eq!(received.payload.as_ptr(), original_payload);
    assert_eq!(received.payload.allocation_bytes(), read.bytes());
    assert!(
        read.try_buffer().is_err(),
        "same actual read cannot be reused while Datagram retains its input range"
    );
    drop(received);
    assert!(
        read.try_buffer().is_ok(),
        "actual standard caller returned that exact read workspace"
    );
    drop(frame);
    drop(tx);
    drop(rx);
    assert!(control
        .wait_closed(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap()
        .is_complete());
    drop(control);
    drop(handoff);
}

pub(super) async fn healthy_actor(
    calls: &Arc<PacketDriverCalls>,
    tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    wire: &zero_platform_tokio::contract::scope::TaskJoinWitness,
) -> (
    zero_platform_tokio::contract::scope::TaskJoinWitness,
    tokio::io::DuplexStream,
) {
    let (mut actual_io, peer) = tokio::io::duplex(8);
    let (ready, pending) = tokio::sync::oneshot::channel();
    let (_, actor) = calls
        .spawn_reconnect(&tasks.downgrade(), wire, move |_| async move {
            let mut byte = [0];
            let reading = actual_io.read_exact(&mut byte);
            tokio::pin!(reading);
            poll_fn(|cx| {
                assert!(reading.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            ready.send(()).unwrap();
            reading
                .await
                .map_err(|e| failure(Stage::Receive, Reason::Io).with_source(e))?;
            Ok(())
        })
        .unwrap();
    pending.await.unwrap();
    (actor, peer)
}
