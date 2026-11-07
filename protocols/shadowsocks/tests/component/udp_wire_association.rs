//! Real native UDP and the original SS codec, with separate caller/wire IDs.
use super::*;
use std::{
    future::{poll_fn, Future},
    num::NonZeroUsize,
    task::Poll,
    time::Duration,
};
use zero_core::{
    component::{
        AssociationId, ComponentId, ComponentInstanceId, NetworkGeneration, ResourceGeneration,
    },
    packet::Datagram,
    request::Destination,
};
use zero_platform_tokio::{
    contract::{
        component::ResourceDeclaration,
        datagram::{recv, send},
        network::{CarrierContext, UpstreamBinding, UpstreamOpener},
        scope::TaskScope,
    },
    controlled_carrier::{CarrierOwner, NativeCarrierOpener},
};
#[tokio::test]
async fn ss_real_borrowed_codec_preserves_logical_caller_and_exact_native_wire_associations() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = peer.local_addr().unwrap();
        let endpoint = Destination::new(Address::Ipv4([127, 0, 0, 1]), remote.port());
        let egress = Arc::new(zero_platform_tokio::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: zero_traits::DialPolicy {
                source_ip: Some(remote.ip()),
                ..Default::default()
            },
            network_generation: NetworkGeneration(egress.generation()),
        };
        let instance = ComponentInstanceId {
            component: ComponentId(916),
            generation: ResourceGeneration(7),
        };
        let host = Arc::new(CancellationToken::new());
        let one = NonZeroUsize::new(1).unwrap();
        let end = || Instant::now() + Duration::from_secs(3);
        let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
        let mut owner = CarrierOwner::new(instance, one, &host);
        owner
            .registration()
            .bind_declared(
                &ResourceDeclaration::ControlledDatagramCarrier {
                    endpoint: endpoint.clone(),
                    binding: binding.clone(),
                    max_sockets: one,
                },
                &scope.registrar(),
                &host,
            )
            .unwrap();
        owner.start(end()).await.unwrap();
        let mut grant = owner.grants().reserve().unwrap();
        let view = grant.claim_transferred_view().unwrap();
        let adoption = grant.adoption();
        let opener =
            NativeCarrierOpener::new(instance, binding.clone(), remote, egress, None).unwrap();
        let native = opener
            .prepare_controlled_datagram(
                &binding,
                endpoint.clone(),
                CarrierContext {
                    session: None,
                    deadline: Some(end()),
                    cancel: CancellationToken::new(),
                },
                grant,
            )
            .unwrap();
        let local = native.local_addr().unwrap();
        let loan = native.into_datagram_loan(view, AssociationId(971)).unwrap();
        let wire = loan.io.wire_association();
        let (raw, origin) = loan.into_session();
        let codec = Arc::new(crate::udp::ShadowsocksDatagramCodec::new(
            crate::shared::CipherKind::Aes128Gcm,
            b"same-secret",
        ));
        let target = Destination::new(Address::Ipv4([203, 0, 113, 8]), 443);
        let mut channel = wrap(
            Raw {
                session: raw,
                endpoint,
                association: wire,
            },
            DatagramRequest {
                association: AssociationId(972),
                targets: zero_core::request::DatagramTargets::Fixed(target.clone()),
            },
            codec.clone(),
            CancellationToken::new(),
            None,
            None,
        );
        let mut frame = Datagram {
            association: AssociationId(971),
            address: target.clone(),
            payload: b"same SS association".to_vec().into(),
        };
        let rejected = send(
            channel.tx.as_mut(),
            channel.control.as_mut(),
            &frame,
            &channel.limits,
        )
        .await
        .unwrap_err();
        assert_eq!(
            (rejected.error.reason, rejected.delivery),
            (Reason::Protocol, DeliveryState::NotSubmitted)
        );
        let mut bytes = [0; 2048];
        {
            let mut pending = Box::pin(peer.recv_from(&mut bytes));
            poll_fn(|cx| {
                assert!(pending.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        frame.association = AssociationId(972);
        for payload in [b"same SS association".as_slice(), b"".as_slice()] {
            frame.payload = payload.to_vec().into();
            let receipt = send(
                channel.tx.as_mut(),
                channel.control.as_mut(),
                &frame,
                &channel.limits,
            )
            .await
            .unwrap();
            assert_eq!(receipt.accepted_bytes, payload.len());
            let (size, source) = peer.recv_from(&mut bytes).await.unwrap();
            assert_eq!(source, local);
            let (address, port, decoded) = codec.decode(&bytes[..size]).unwrap();
            assert_eq!((address, port), (target.address.clone(), target.port));
            assert_eq!(&decoded, payload);
            let response = codec.encode(&target.address, target.port, payload).unwrap();
            peer.send_to(&response, source).await.unwrap();
            let received = recv(channel.rx.as_mut()).await.unwrap().unwrap();
            assert_eq!(received.association, AssociationId(972));
            assert_eq!(received.address, target);
            assert_eq!(&*received.payload, payload);
        }
        assert!(origin.driver().is_none() && origin.closed().is_none());
        assert!(!channel.control.try_close().unwrap().is_complete());
        drop(channel.tx);
        drop(channel.rx);
        assert!(channel
            .control
            .wait_closed(end())
            .await
            .unwrap()
            .is_complete());
        let closed = origin.closed().unwrap();
        assert!(closed.matches(&origin) && closed.had_native_acquisition());
        assert!(adoption.users_closed().unwrap().matches(&adoption));
        assert!(owner.shutdown(end()).await.unwrap().is_complete());
        let report = scope.shutdown(end()).await;
        assert!(report.is_complete() && report.exited_tasks.is_empty());
    })
    .await
    .unwrap();
}
