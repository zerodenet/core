use super::*;

#[tokio::test]
async fn actual_bound_peer_recycles_one_native_and_binding_slot_across_network_epochs_before_peer_exit(
) {
    let live = LiveDirectSource::new().await;
    let one = NonZeroUsize::new(1).unwrap();
    let retirement = live.proxy.retired_datagrams.clone();
    let ((mut bridge, endpoint, driver, resources), authority) = live
        .context
        .claim_bound_outbound_datagram(deadline(), one, one, |grant| {
            let mut grants = limits();
            grants.native_sockets = one;
            let (owner, driver) = DirectEndpointOwner::for_bound_peer(
                DirectBoundPeerOptions {
                    identity: DirectEndpointIdentity {
                        resource: grant.identity.instance,
                        ingress: live.context.ingress(),
                        dispatcher: grant.identity.association,
                    },
                    task: grant.identity.task,
                    queue: UdpReceiveQueueConfig::default(),
                    limits: grants,
                },
                live.context.binding(),
                None,
            )?;
            let endpoint = owner.endpoint();
            Ok((
                zero_platform_tokio::contract::lifecycle::erase_lifecycle(owner),
                (
                    DirectDispatchEndpoint::new(endpoint.clone(), retirement),
                    endpoint,
                    driver,
                    grant.resources,
                ),
            ))
        })
        .await
        .unwrap();
    live.driver_slot.submit(driver).unwrap();
    let egress = live.runtime.services().upstream().egress_control();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut prior: Option<DirectUdpResponseSource> = None;
    for epoch in 0..32 {
        if epoch != 0 {
            egress.invalidate_network();
            assert!(!prior.take().unwrap().is_current());
            // No pending/new UDP response is needed to wake the actual driver.
            endpoint.refresh_sources(deadline()).await.unwrap();
        }
        let generation = NetworkGeneration(egress.generation());
        let authorized = authority
            .authorize(live.source().await, generation)
            .unwrap();
        let binding = as_binding(
            &resources,
            authorized.id,
            generation,
            authorized.policy,
            authorized.source,
        );
        let (session, _handle) = business(&live, peer.local_addr().unwrap()).await;
        bridge
            .admit_bound(
                SessionId(session.id),
                binding,
                None,
                CancellationToken::new(),
            )
            .unwrap();
        let original = send_bridge(&mut bridge, SessionId(session.id), &peer, b"").await;
        peer.send_to(b"", original).await.unwrap();
        let mut bytes = [0; 8];
        let (size, source) = bridge.recv_from_addr(&mut bytes).await.unwrap();
        assert_eq!(size, 0);
        assert!(source.is_current());
        if epoch == 0 {
            // A stale command uses the actual old authorization. Its exact
            // NotSubmitted/Never source returns without faulting the whole peer.
            egress.invalidate_network();
            let stale = bridge
                .send_new_packet(
                    SessionId(session.id),
                    &Address::Ipv4([127, 0, 0, 1]),
                    &[peer.local_addr().unwrap()],
                    b"must not submit",
                )
                .await
                .unwrap_err();
            assert_rejection(&stale, Reason::StalePlan);
            assert!(!live.context.binding().cancellation().is_cancelled());
        }
        assert!(bridge
            .finish_session(SessionId(session.id), deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(!source.is_current());
        tokio::net::UdpSocket::bind(original).await.unwrap();
        prior = Some(source);
    }
    assert_eq!(
        live.context
            .binding()
            .wait_joined(Instant::now() + Duration::from_millis(1))
            .await
            .unwrap_err()
            .reason,
        Reason::Timeout
    );
    drop(bridge);
    drop(resources);
    drop(endpoint);
    live.close().await;
}
