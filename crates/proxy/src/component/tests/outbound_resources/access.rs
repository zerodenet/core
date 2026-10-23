use super::*;
use zero_platform_tokio::contract::component::ResourceAccess;

#[tokio::test]
async fn actual_udp_grant_requires_bound_live_task_exact_request_and_one_native_attempt() {
    timeout(Duration::from_secs(3), async {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = peer.local_addr().unwrap();
        let control = EgressInterfaceControl::default();
        let parent = CancellationToken::new();
        let grants = provider(target, &control, parent.clone());
        let req = request(target, &control);
        assert_eq!(
            grants.open_outbound_datagram(req).unwrap_err().reason,
            Reason::InvalidConfig
        );
        assert!(!grants.state().attempted);
        let (mut scope, task) = binding(tokio::runtime::Handle::current());
        grants.activate(task).unwrap();
        // Lazy opening remains allowed after activation has returned; there is
        // no borrowed start-phase guard or hidden resource task.
        tokio::task::yield_now().await;
        let other = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut wrong = req;
        wrong.peer = other.local_addr().unwrap();
        assert_eq!(
            grants.open_outbound_datagram(wrong).unwrap_err().reason,
            Reason::InvalidConfig
        );
        wrong = req;
        wrong.expected_network_generation = req.expected_network_generation.checked_next().unwrap();
        assert_eq!(
            grants.open_outbound_datagram(wrong).unwrap_err().reason,
            Reason::InvalidConfig
        );
        assert!(!grants.state().attempted);
        // Install the exact returned pair synchronously before the first await.
        let mut installed = Some(grants.open_outbound_datagram(req).unwrap());
        let bound = installed.as_ref().unwrap().socket.local_addr().unwrap();
        assert_eq!(
            installed.as_ref().unwrap().selection.generation(),
            control.generation()
        );
        assert_eq!(
            grants.open_outbound_datagram(req).unwrap_err().reason,
            Reason::Capacity
        );
        assert!(grants.fault().is_none());
        assert!(!grants.native_unconfirmed());
        assert_eq!(
            installed
                .as_ref()
                .unwrap()
                .socket
                .send_to_addr(b"", target)
                .await
                .unwrap(),
            0
        );
        let mut bytes = [0; 16];
        let (size, sender) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(size, 0);
        assert_eq!(sender.port(), bound.port());
        peer.send_to(b"reply", sender).await.unwrap();
        let (size, from) = installed
            .as_ref()
            .unwrap()
            .socket
            .recv_from_addr(&mut bytes)
            .await
            .unwrap();
        assert_eq!(&bytes[..size], b"reply");
        assert_eq!(from, target);
        let exit = scope.shutdown(deadline()).await;
        assert!(exit.is_complete());
        assert!(exit.exited_tasks.contains(&task));
        grants.revoke(); // The host revokes authority after its actual task exit.
        assert!(!parent.is_cancelled());
        assert_eq!(
            grants.open_outbound_datagram(req).unwrap_err().reason,
            Reason::Revoked
        );
        let provider_report = grants.shutdown_io().unwrap();
        assert!(provider_report.is_complete());
        assert!(
            provider_report.closed_io.is_empty(),
            "transferred fd is not the provider's close receipt"
        );
        assert_eq!(
            std::net::UdpSocket::bind(bound).unwrap_err().kind(),
            std::io::ErrorKind::AddrInUse
        );
        installed.take().unwrap().socket.close().unwrap();
        std::net::UdpSocket::bind(bound).unwrap();
        assert!(grants.activate(task).is_err());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn acquired_socket_stays_in_its_receiver_owner_after_topology_becomes_stale() {
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let control = EgressInterfaceControl::default();
    let grants = provider(target, &control, CancellationToken::new());
    let (mut scope, task) = binding(tokio::runtime::Handle::current());
    grants.activate(task).unwrap();
    let installed = grants
        .open_outbound_datagram(request(target, &control))
        .unwrap();
    let bound = installed.socket.local_addr().unwrap();
    let selected_generation = installed.selection.generation();
    control.invalidate_network();
    assert_ne!(selected_generation, control.generation());
    assert!(grants.fault().is_none());
    assert_eq!(
        std::net::UdpSocket::bind(bound).unwrap_err().kind(),
        std::io::ErrorKind::AddrInUse
    );
    assert!(scope
        .shutdown(deadline())
        .await
        .exited_tasks
        .contains(&task));
    grants.revoke();
    installed.socket.close().unwrap();
    std::net::UdpSocket::bind(bound).unwrap();
}

#[test]
fn existing_static_resources_and_default_access_do_not_grant_dynamic_outbound_sockets() {
    let control = EgressInterfaceControl::default();
    let target = "127.0.0.1:53".parse().unwrap();
    let decl = declaration(target, &control);
    assert!(
        matches!(crate::component::instance::HostResources::new(instance().component, std::slice::from_ref(&decl), CancellationToken::new()), Err(failure) if failure.reason == Reason::Unsupported)
    );
    let empty = crate::component::instance::HostResources::new(
        instance().component,
        &[],
        CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(
        empty
            .open_outbound_datagram(request(target, &control))
            .unwrap_err()
            .reason,
        Reason::InvalidConfig
    );
    assert_eq!(
        crate::component::instance::NoResources
            .open_outbound_datagram(request(target, &control))
            .unwrap_err()
            .reason,
        Reason::InvalidConfig
    );
    let mut too_many = decl;
    if let ResourceDeclaration::OutboundDatagram { max_sockets, .. } = &mut too_many {
        *max_sockets = NonZeroUsize::new(2).unwrap();
    }
    assert!(
        matches!(HostOutboundDatagramResources::new(instance(), &too_many, control, CancellationToken::new()), Err(failure) if failure.reason == Reason::Unsupported)
    );
}
