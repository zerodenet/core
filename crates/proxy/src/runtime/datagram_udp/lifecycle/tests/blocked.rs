//! Real routing must not publish a response owner for an already finished flow.
use super::*;
use crate::runtime::Proxy;
use zero_config::RuntimeConfig;

struct Responder;

#[async_trait::async_trait]
impl DatagramUdpResponder<()> for Responder {
    async fn read_inbound_dispatch(
        &mut self,
        _: &(),
    ) -> Result<Option<InboundUdpDispatch>, zero_core::Error> {
        unreachable!("the actual routed packet is supplied directly")
    }

    fn on_dispatch_success(&mut self, _: u64, _: &InboundUdpDispatch) {
        panic!("a blocked session has no live response flow");
    }

    async fn write_response_for_session(
        &mut self,
        _: &(),
        _: Option<u64>,
        _: &Address,
        _: u16,
        _: &[u8],
    ) -> Result<Option<usize>, zero_core::Error> {
        unreachable!("a blocked flow has no original response IO")
    }
}

#[tokio::test]
async fn blocked_datagram_finishes_without_publishing_a_live_response_mapping() {
    let config =
        RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"reject"}}}"#).unwrap();
    let proxy = Proxy::new(config).unwrap();
    let running = proxy.spawn();
    running
        .wait_ready(tokio::time::Instant::now() + std::time::Duration::from_secs(3))
        .await
        .unwrap();
    crate::runtime::udp_ingress::test_source::run(&proxy, |proxy, runtime| async move {
        let mut dispatch = runtime.new_dispatch("blocked-test").await.unwrap();
        let context = DatagramUdpLoopContext {
            runtime: &runtime,
            auth: None,
        };
        let packet = InboundUdpDispatch::new(
            zero_core::ProtocolType::new("blocked-test"),
            Address::Ipv4([203, 0, 113, 9]),
            5355,
            b"blocked input".to_vec(),
            None,
        );
        assert!(
            super::super::read::process_datagram_read::<(), _>(
                &context,
                &mut dispatch,
                &mut Responder,
                Ok(Some(packet)),
            )
            .await
        );
        assert!(proxy.engine().active_sessions().is_empty());
        let completed = proxy.engine().completed_sessions();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].outcome, zero_engine::SessionOutcome::Blocked);
        assert!(!dispatch.contains_session(completed[0].id));
        assert!(dispatch.finish_all().is_empty());
    })
    .await;
    running.shutdown().await.unwrap();
}
