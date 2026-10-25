use super::*;
use zero_config::{MieruTransport, OutboundProtocolConfig};
use zero_core::{
    component::AssociationId,
    packet::Datagram,
    request::{DatagramTargets, Destination},
    Address,
};
use zero_platform_tokio::contract::datagram::{recv, send, DatagramRequest};

#[tokio::test]
async fn packet_underlay_shares_real_tcp_udp_roles_and_waits_for_old_users_before_replacement() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let server = Server::packet().await;
        let fixture = Assembly::new(server.address);
        let mut config = (*fixture.config()).clone();
        for outbound in &mut config.outbounds {
            if let OutboundProtocolConfig::Mieru { transport, .. } = &mut outbound.protocol {
                *transport = MieruTransport::Udp;
            }
        }
        let config = Arc::new(config);
        let host = &fixture.catalog;
        host.drive_supervised(async {
            host.start(config.clone(), deadline()).await.unwrap();
            host.commit(&config, || Ok(())).unwrap();
            let slot = host.current(&config).unwrap().direct(Some("a")).unwrap();
            let mut tcp = fixture.open(&slot).await;
            let mut udp = slot
                .datagram(deadline())
                .await
                .unwrap()
                .open(
                    DatagramRequest {
                        association: AssociationId(71),
                        targets: DatagramTargets::Multiple,
                    },
                    fixture.execution_context(&slot),
                )
                .await
                .unwrap();
            for payload in [b"packet-carried".as_slice(), b"".as_slice()] {
                let frame = Datagram {
                    address: Destination::new(Address::Ipv4([203, 0, 113, 9]), 53),
                    association: AssociationId(71),
                    payload: payload.to_vec().into(),
                };
                send(udp.tx.as_mut(), udp.control.as_mut(), &frame, &udp.limits)
                    .await
                    .unwrap();
                assert_eq!(recv(udp.rx.as_mut()).await.unwrap().unwrap(), frame);
            }
            tcp.write_all(b"same-native").await.unwrap();
            let mut echoed = [0; 11];
            tcp.read_exact(&mut echoed).await.unwrap();
            assert_eq!(&echoed, b"same-native");
            assert_eq!(fixture.pool.snapshot().connections, 1);
            assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
            let mut changed = (*config).clone();
            if let OutboundProtocolConfig::Mieru { password, .. } =
                &mut changed.outbounds[1].protocol
            {
                *password = "unused-replaced".into();
            }
            let changed = Arc::new(changed);
            let mut replacement = Box::pin(host.start(changed.clone(), deadline()));
            std::future::poll_fn(|cx| {
                assert!(replacement.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(tcp);
            drop(udp.tx);
            drop(udp.control);
            assert!(
                tokio::time::timeout(Duration::from_millis(30), replacement.as_mut())
                    .await
                    .is_err()
            );
            assert_eq!(fixture.builds.load(Ordering::SeqCst), 1);
            drop(udp.rx);
            replacement.await.unwrap();
            host.commit(&changed, || Ok(())).unwrap();
            assert_eq!(fixture.builds.load(Ordering::SeqCst), 2);
            assert_eq!(
                fixture.closes.load(Ordering::SeqCst),
                1,
                "old original packet controller closed before new owner build"
            );
            let fresh = host.current(&changed).unwrap().direct(Some("a")).unwrap();
            let mut stream = fixture.open(&fresh).await;
            stream.write_all(b"new").await.unwrap();
            stream.read_exact(&mut [0; 3]).await.unwrap();
            assert_eq!(fixture.pool.snapshot().connections, 1);
            drop(stream);
            host.shutdown(deadline()).await.unwrap();
            assert_eq!(fixture.closes.load(Ordering::SeqCst), 2);
        })
        .await;
        server.close().await;
    })
    .await
    .unwrap();
}
