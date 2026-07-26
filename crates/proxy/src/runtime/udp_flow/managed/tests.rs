use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::model::ManagedDatagramExistingSend;
use super::ManagedDatagramFlowHandler;
use super::{ManagedUdpFlowResume, ManagedUdpHandlers};
use crate::runtime::udp_flow::outbound::{ManagedUdpFlowRef, UdpFlowOutbound};
use crate::runtime::udp_flow::registered::{
    RegisteredUdpHandlers, RegisteredUdpState, UpstreamUdpHandlers,
};
use crate::runtime::udp_flow::result::FlowFailure;
use crate::runtime::udp_flow::snapshot::UdpFlowSnapshot;
use zero_config::RuntimeConfig;
use zero_core::{Address, Network, ProtocolType, Session};

#[derive(Debug, Clone, PartialEq, Eq)]
struct FakeResume {
    connection_id: u64,
    endpoint: &'static str,
}

struct FakeDatagramHandler {
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl ManagedDatagramFlowHandler for FakeDatagramHandler {
    fn supports_managed_existing(&self, resume: &ManagedUdpFlowResume) -> bool {
        resume.as_ref::<FakeResume>().is_some()
    }

    async fn send_managed_existing(
        &mut self,
        request: ManagedDatagramExistingSend<'_>,
    ) -> Result<usize, FlowFailure> {
        let resume = request
            .resume
            .as_ref::<FakeResume>()
            .expect("fake resume type");
        assert_eq!(resume.connection_id, 17);
        assert_eq!(request.session_id, 23);
        assert_eq!(request.server, "udp-upstream.test");
        assert_eq!(request.port, 5353);
        assert_eq!(request.target, &Address::Domain("target.test".to_owned()));
        assert_eq!(request.target_port, 53);
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(request.payload.len())
    }
}

#[test]
fn managed_udp_state_registers_and_restores_opaque_resumes() {
    let mut state = RegisteredUdpState::new(RegisteredUdpHandlers {
        managed: ManagedUdpHandlers {
            datagram: Vec::new(),
            stream_packet: Vec::new(),
            relay: Vec::new(),
        },
        upstream: UpstreamUdpHandlers {
            upstream: Vec::new(),
        },
    });
    let first = FakeResume {
        connection_id: 7,
        endpoint: "first.test",
    };
    let second = FakeResume {
        connection_id: 9,
        endpoint: "second.test",
    };

    let first_ref = state.register_managed_flow(ManagedUdpFlowResume::new(first.clone()));
    let second_ref = state.register_managed_flow(ManagedUdpFlowResume::new(second.clone()));

    assert_ne!(first_ref, second_ref);
    assert_eq!(
        state
            .managed_flow_resume(first_ref)
            .and_then(|resume| resume.cloned::<FakeResume>()),
        Some(first)
    );
    assert_eq!(
        state
            .managed_flow_resume(second_ref)
            .and_then(|resume| resume.cloned::<FakeResume>()),
        Some(second)
    );
    assert!(state
        .managed_flow_resume(ManagedUdpFlowRef::new(u64::MAX))
        .is_none());
}

#[tokio::test]
async fn registered_udp_state_forwards_with_restored_opaque_resume() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut state = RegisteredUdpState::new(RegisteredUdpHandlers {
        managed: ManagedUdpHandlers {
            datagram: vec![Box::new(FakeDatagramHandler {
                calls: calls.clone(),
            })],
            stream_packet: Vec::new(),
            relay: Vec::new(),
        },
        upstream: UpstreamUdpHandlers {
            upstream: Vec::new(),
        },
    });
    let flow_ref = state.register_managed_flow(ManagedUdpFlowResume::new(FakeResume {
        connection_id: 17,
        endpoint: "udp-upstream.test",
    }));
    let flow = UdpFlowSnapshot {
        session: Session::new(
            23,
            Address::Domain("target.test".to_owned()),
            53,
            Network::Udp,
            ProtocolType::UNKNOWN,
        ),
        outbound: UdpFlowOutbound::Datagram {
            tag: "fake-datagram".to_owned(),
            server: "udp-upstream.test".to_owned(),
            port: 5353,
            managed: flow_ref,
        },
        client_session_id: None,
        passive_relay_selections: Vec::new(),
        rate_limiters: crate::runtime::udp_flow::rate_limit::UdpFlowRateLimiters::default(),
    };
    let config =
        RuntimeConfig::parse(r#"{ "route": { "rules": [], "final": { "type": "direct" } } }"#)
            .expect("minimal runtime config");
    let proxy = crate::runtime::Proxy::new(config).expect("minimal proxy");
    let mut chain_tasks = tokio::task::JoinSet::new();
    let payload = b"forwarded payload";

    let sent = match state
        .forward_existing_managed_flow(
            &mut chain_tasks,
            proxy.udp_runtime_services(),
            (&flow, payload),
        )
        .await
    {
        Ok(sent) => sent,
        Err(_) => panic!("registered managed forward failed"),
    };

    assert_eq!(sent, payload.len());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
