use super::*;
use zero_core::{component::AssociationId, request::DatagramTargets, request::Destination};
use zero_platform_tokio::{
    contract::{datagram::DatagramRequest, Reason, Stage},
    controlled_carrier::CarrierOriginOwner,
};
#[derive(Clone, Default)]
struct Budgets(Arc<Mutex<Vec<(u64, u64)>>>);
struct Fields {
    entries: Option<u64>,
    failures: Option<u64>,
}
impl tracing::field::Visit for Fields {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "entries" => self.entries = Some(value),
            "failure_groups" => self.failures = Some(value),
            _ => {}
        }
    }
    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}
impl tracing::Subscriber for Budgets {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == "hysteria2::pool_budget"
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut fields = Fields {
            entries: None,
            failures: None,
        };
        event.record(&mut fields);
        if let (Some(entries), Some(failures)) = (fields.entries, fields.failures) {
            self.0.lock().unwrap().push((entries, failures));
        }
    }
}
fn context(proxy: &Proxy, slot: &crate::component::instance::ComponentSlot) -> OpenContext {
    OpenContext {
        identity: RequestIdentity::Service(Arc::new(zero_core::request::ServiceRequestFacts {
            origin: zero_core::request::ServiceOrigin::Runtime,
            purpose: zero_core::request::ServicePurpose::BackgroundControl,
        })),
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
    }
}
fn actual_peer_close_source(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if matches!(
            error.downcast_ref::<quinn::ConnectionError>(),
            Some(quinn::ConnectionError::ApplicationClosed(_))
        ) {
            return true;
        }
        current = error.source();
    }
    false
}
#[tokio::test]
async fn hysteria2_real_peer_failures_keep_finite_unpublished_positions_until_host_shutdown() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let budgets=Budgets::default();let _capture=tracing::subscriber::set_default(budgets.clone());
        let server = Server::new().await;
        let proxy = server.proxy();
        start(&proxy).await;
        let services = proxy.tcp_runtime_services().execution();
        let slot = services
            .components
            .as_ref()
            .unwrap()
            .configured("hy")
            .unwrap();
        let role = slot.datagram(end()).await.unwrap();
        let request = |id| DatagramRequest {
            association: AssociationId(id),
            targets: DatagramTargets::Fixed(Destination::new(Address::Ipv4([203, 0, 113, 11]), 53)),
        };
        let mut saved_root = None;
        for id in 1..=256 {
            let loan = proxy
                .protocols
                .drive_component_supervision(role.open_carrier(request(id), context(&proxy, &slot)))
                .await
                .unwrap();
            if saved_root.is_none() {
                saved_root = Some(loan.owner.original().clone());
            }
            let zero_platform_tokio::controlled_carrier::loan::DatagramLoan { io, mut owner } =
                loan;
            // This error comes from an actual authenticated QUIC peer. The
            // original dispatcher really terminates, rather than a fake fault
            // provider or a synthetic successful TaskExit.
            server.retire_current(id as usize).await;
            let mut bytes = [0; 1];
            assert!(proxy
                .protocols
                .drive_component_supervision(io.receive(&mut bytes))
                .await
                .is_err());
            drop(io);
            let report = owner.close(end()).await.unwrap();
            assert!(report.is_complete());
            drop(owner);
            assert_eq!(server.authenticated.load(Ordering::SeqCst), id as usize);
        }
        let failure = match proxy
            .protocols
            .drive_component_supervision(role.open(request(257), context(&proxy, &slot)))
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("unpublished original task failures lost their finite positions"),
        };
        assert_eq!(failure.reason, Reason::Capacity);
        assert_eq!(
            server.authenticated.load(Ordering::SeqCst),
            256,
            "no new authentication may bypass retained original failures"
        );
        assert!(budgets.0.lock().unwrap().contains(&(0,256)),"actual map entries and retained fault groups are distinct original budget positions");
        assert!(budgets.0.lock().unwrap().iter().all(|(entries,failures)|entries+failures<=256));
        let root = saved_root.unwrap();
        assert!(matches!(root.owner(),CarrierOriginOwner::Metadata(owner) if owner.current_original().is_none()),"all original slots are closed while the fault ledger remains full");
        assert!(
            root.closed().is_none(),
            "old slot closure is not whole-pool closure"
        );
        let failed = proxy
            .protocols
            .shutdown_components(end())
            .await
            .unwrap_err();
        let channel = crate::transport::channel_failure(&failed)
            .expect("host delivered the original failed task");
        assert_eq!(
            (channel.stage, channel.reason),
            (Stage::Receive, Reason::Protocol)
        );
        assert!(
            actual_peer_close_source(&failed),
            "host publication preserves the original typed QUIC peer error"
        );
        assert!(
            root.closed().is_some(),
            "actual native closure is checked independently of the published task failure"
        );
        server.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn hysteria2_healthy_confirmed_evictions_reuse_more_than_twice_the_entry_capacity() {
    tokio::time::timeout(Duration::from_secs(60),async {
        let budgets=Budgets::default();let _capture=tracing::subscriber::set_default(budgets.clone());
        let server=Server::new().await;
        let outbounds=(0..513).map(|id|serde_json::json!({"tag":format!("hy-{id}"),"protocol":{"type":"hysteria2","server":"127.0.0.1","port":server.port,"password":"p","insecure":true}})).collect::<Vec<_>>();
        let proxy=Proxy::new(zero_config::RuntimeConfig::parse(&serde_json::json!({"outbounds":outbounds,"route":{"final":{"type":"route","outbound":"hy-0"}}}).to_string()).unwrap()).unwrap();
        start(&proxy).await;
        let services=proxy.tcp_runtime_services().execution();let plan=services.components.as_ref().unwrap();
        let mut first=None;
        for id in 0..513 {
            let slot=plan.configured(&format!("hy-{id}")).unwrap();let role=slot.datagram(end()).await.unwrap();
            let loan=proxy.protocols.drive_component_supervision(role.open_carrier(DatagramRequest {association:AssociationId(id as u64+1),targets:DatagramTargets::Fixed(Destination::new(Address::Ipv4([203,0,113,12]),53))},context(&proxy,&slot))).await.unwrap_or_else(|error|panic!("healthy admission round {id}: {error}; actual budget {:?}",budgets.0.lock().unwrap().last()));
            if first.is_none() {first=Some(match loan.owner.original().owner() {CarrierOriginOwner::Metadata(owner)=>owner.current_original().unwrap(),_=>panic!("actual HY2 metadata original")});}
            let zero_platform_tokio::controlled_carrier::loan::DatagramLoan {io,mut owner}=loan;drop(io);
            assert!(owner.close(end()).await.unwrap().is_complete());drop(owner);
        }
        assert!(budgets.0.lock().unwrap().iter().all(|(entries,failures)|*entries<=256 && *failures==0));
        assert!(budgets.0.lock().unwrap().iter().any(|(entries,_)|*entries==256));
        assert_eq!(server.authenticated.load(Ordering::SeqCst),513,"settled successes have no lifetime failure charge");
        assert!(first.unwrap().closed().unwrap().had_native_acquisition(),"pressure reclaimed the actual first original, with its native receipt");
        proxy.protocols.shutdown_components(end()).await.unwrap();server.close().await;
    }).await.unwrap();
}
