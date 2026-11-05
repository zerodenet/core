//! Production configuration bounds the same actual HY2 owner across profiles.
use super::*;
use zero_core::{component::AssociationId, request::DatagramTargets, request::Destination};
use zero_platform_tokio::{
    contract::{datagram::DatagramRequest, Reason},
    controlled_carrier::{loan::DatagramLoan, CarrierOriginOwner},
};

async fn open(proxy: &Proxy, tag: &str, id: u64) -> Result<DatagramLoan, ChannelError> {
    let services = proxy.tcp_runtime_services().execution();
    let slot = services
        .components
        .as_ref()
        .unwrap()
        .configured(tag)
        .unwrap();
    let role = slot.datagram(end()).await.unwrap();
    proxy
        .protocols
        .drive_component_supervision(role.open_carrier(
            DatagramRequest {
                association: AssociationId(id),
                targets: DatagramTargets::Fixed(Destination::new(
                    Address::Ipv4([203, 0, 113, 2]),
                    53,
                )),
            },
            super::budget::context(proxy, &slot),
        ))
        .await
}
async fn echo(loan: &DatagramLoan) {
    loan.io.send(b"configured budget").await.unwrap();
    let mut bytes = [0; 32];
    let count = loan.io.receive(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..count], b"configured budget");
}
async fn returned(loan: DatagramLoan) {
    let DatagramLoan { io, mut owner } = loan;
    drop(io);
    assert!(owner.close(end()).await.unwrap().is_complete());
    assert!(owner.closed().unwrap().matches(&owner));
    drop(owner);
}

#[tokio::test]
async fn hysteria2_configured_actual_entry_budget_rejects_active_pressure_reclaims_and_replaces() {
    tokio::time::timeout(Duration::from_secs(25), async {
        let server = Server::new().await;
        let mut config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "runtime":{"hysteria2_max_entries":1},
            "outbounds":[
                {"tag":"hy","protocol":{"type":"hysteria2","server":"127.0.0.1","port":server.port,"password":"p","insecure":true}},
                {"tag":"other","protocol":{"type":"hysteria2","server":"127.0.0.1","port":server.port,"password":"p","insecure":true}}
            ],
            "route":{"final":{"type":"route","outbound":"hy"}}
        }).to_string()).unwrap();
        let (proxy, running, handle) = super::reload::running(config.clone()).await;
        assert_eq!(server.authenticated.load(Ordering::SeqCst), 0, "capacity is not native preallocation");
        let first = open(&proxy, "hy", 1).await.unwrap();
        echo(&first).await;
        let root = first.owner.original().clone();
        let prior = match root.owner() {
            CarrierOriginOwner::Metadata(owner) => owner.current_original().unwrap(),
            _ => panic!("actual metadata original"),
        };
        let error = match open(&proxy, "other", 2).await {
            Ok(_) => panic!("a different profile cannot evict active original users"),
            Err(error) => error,
        };
        assert_eq!(error.reason, Reason::Capacity);
        assert_eq!(server.authenticated.load(Ordering::SeqCst), 1);
        let same = open(&proxy, "hy", 3).await.unwrap();
        assert!(same.owner.original().same_original(&root));
        returned(same).await;
        echo(&first).await;
        returned(first).await;
        assert!(prior.closed().is_none(), "logical return alone does not refund native capacity");
        let second = open(&proxy, "other", 4).await.unwrap();
        echo(&second).await;
        assert!(prior.closed().unwrap().had_native_acquisition(), "pressure confirmed the exact old native original before admitting its successor");
        assert_eq!(server.authenticated.load(Ordering::SeqCst), 2);
        let second_root = second.owner.original().clone();
        let second_prior = match second_root.owner() {
            CarrierOriginOwner::Metadata(owner) => owner.current_original().unwrap(),
            _ => panic!("actual metadata original"),
        };
        let admission = proxy.tcp_runtime_services().execution().components.as_ref().unwrap().configured("other").unwrap().service_admission().unwrap();
        config.runtime.hysteria2_max_entries = Some(2);
        let applying = tokio::spawn(async move {
            handle.apply_runtime_config_and_wait(config, Duration::from_secs(5)).await
        });
        admission.revoked().await;
        assert!(!applying.is_finished(), "the old actual logical view blocks budget replacement");
        assert!(second_prior.closed().is_none());
        returned(second).await;
        applying.await.unwrap().unwrap();
        assert!(second_prior.closed().unwrap().had_native_acquisition());
        assert!(second_root.closed().unwrap().matches(&second_root));
        assert_eq!(server.authenticated.load(Ordering::SeqCst), 2, "the replacement remains cold");
        let a = open(&proxy, "hy", 5).await.unwrap();
        let b = open(&proxy, "other", 6).await.unwrap();
        echo(&a).await;
        echo(&b).await;
        assert_ne!(b.owner.original().instance(), second_root.instance(), "the configured budget participates in actual pool identity");
        assert_eq!(server.authenticated.load(Ordering::SeqCst), 4);
        returned(a).await;
        returned(b).await;
        running.shutdown().await.unwrap();
        server.close().await;
    }).await.unwrap();
}
