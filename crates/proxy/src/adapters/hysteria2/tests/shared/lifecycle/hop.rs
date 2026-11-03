use super::*;
use zero_core::{component::AssociationId, request::DatagramTargets, request::Destination};
use zero_platform_tokio::{
    contract::{datagram::DatagramRequest, CloseReason},
    controlled_carrier::{loan::DatagramLoan, CarrierOriginOwner},
};

#[tokio::test]
async fn hysteria2_native_hop_closes_the_actual_old_original_and_new_donors_use_the_current_one() {
    tokio::time::timeout(Duration::from_secs(25), async {
        let server = Server::new().await;
        let proxy = Proxy::new(zero_config::RuntimeConfig::parse(&serde_json::json!({
            "outbounds":[{"tag":"hy","protocol":{"type":"hysteria2","server":"127.0.0.1",
                "port":server.port,"password":"p","insecure":true,"transport":{
                    "udp_hop":{"ports":[server.port],"interval_min_secs":5,"interval_max_secs":5},
                    "quic":{"keep_alive_interval_secs":2}}}}],
            "route":{"final":{"type":"route","outbound":"hy"}}
        }).to_string()).unwrap()).unwrap();
        start(&proxy).await;
        let services = proxy.tcp_runtime_services().execution();
        let slot = services
            .components
            .as_ref()
            .unwrap()
            .configured("hy")
            .unwrap();
        let role = slot.datagram(end()).await.unwrap();
        let endpoint = Destination::new(Address::Ipv4([203, 0, 113, 8]), 53);
        let open = |id| {
            role.open_carrier(
                DatagramRequest {
                    association: AssociationId(id),
                    targets: DatagramTargets::Fixed(endpoint.clone()),
                },
                OpenContext {
                    identity: RequestIdentity::Service(Arc::new(
                        zero_core::request::ServiceRequestFacts {
                            origin: zero_core::request::ServiceOrigin::Runtime,
                            purpose: zero_core::request::ServicePurpose::BackgroundControl,
                        },
                    )),
                    deadline: Some(end()),
                    cancel: CancellationToken::new(),
                    observer: None,
                    upstream: UpstreamTransport::new(
                        UpstreamBinding {
                            prefix: vec![],
                            dial: slot.dial.clone(),
                            network_generation: zero_core::component::NetworkGeneration(
                                proxy.egress_interface.generation(),
                            ),
                        },
                        Arc::new(Denied),
                    ),
                },
            )
        };
        let mut old = open(710).await.unwrap();
        let root = old.owner.original().clone();
        let first = match root.owner() {
            CarrierOriginOwner::Metadata(owner) => owner.current_original().unwrap(),
            _ => panic!("HY2 must name its actual metadata original"),
        };
        assert!(root.endpoints().unwrap().matches_metadata(&first));
        async fn echo(loan: &DatagramLoan, payload: &[u8]) {
            loan.io.send(payload).await.unwrap();
            let mut response = [0; 64];
            let size = loan.io.receive(&mut response).await.unwrap();
            assert_eq!(&response[..size], payload);
        }
        echo(&old, b"before hop").await;
        let current = loop {
            let current = match root.owner() {
                CarrierOriginOwner::Metadata(owner) => owner.current_original(),
                _ => unreachable!(),
            };
            if let Some(current) = current {
                if !current.same_original(&first) {
                    break current;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert!(
            first.closed().unwrap().had_native_acquisition(),
            "actual previous native receipt must exist before a replacement is observable"
        );
        assert!(
            !root.is_retired(),
            "same-binding hop preserves the original stable logical pool root"
        );
        assert!(root.endpoints().unwrap().matches_metadata(&current));
        echo(&old, b"after hop").await;
        let mut new = open(711).await.unwrap();
        assert!(
            !new.owner.original().is_retired(),
            "a donor opened after rotation must use an actually acquired current original"
        );
        assert!(new
            .owner
            .original()
            .endpoints()
            .unwrap()
            .matches_metadata(&current));
        echo(&new, b"new donor").await;
        assert_eq!(
            server.authenticated.load(Ordering::SeqCst),
            1,
            "native rotation does not allocate another QUIC authentication pool"
        );
        old.owner.cancel(CloseReason::Shutdown);
        drop(old.io);
        assert!(old.owner.close(end()).await.unwrap().is_complete());
        drop(old.owner);
        new.owner.cancel(CloseReason::Shutdown);
        drop(new.io);
        assert!(new.owner.close(end()).await.unwrap().is_complete());
        drop(new.owner);
        assert!(
            root.closed().is_none(),
            "logical loan close does not close the donor's original pool"
        );
        proxy.protocols.shutdown_components(end()).await.unwrap();
        assert!(
            root.closed().is_some(),
            "whole owner closure supplies the separate final native proof"
        );
        server.close().await;
    })
    .await
    .unwrap();
}
