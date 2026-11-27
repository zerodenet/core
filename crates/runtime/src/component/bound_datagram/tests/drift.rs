//! Same-generation egress selection drift diagnostics and single-bump fencing.
use super::retirement::{declaration, instance};
use super::*;
use std::{error::Error as _, net::SocketAddr};
use zero_platform_tokio::EgressInterfaceControl;
use zero_traits::DialPolicy;

struct DriftPolicy {
    policy: DialPolicy,
    egress: EgressInterfaceControl,
    drifted: std::sync::atomic::AtomicBool,
}

impl DatagramBindingAuthorization for DriftPolicy {
    fn policy(&self) -> &DialPolicy {
        &self.policy
    }
    fn preferred_port(&self) -> Option<u16> {
        None
    }
    fn check_current(&self, expected: NetworkGeneration) -> Result<(), ChannelError> {
        if expected.0 == self.egress.generation() {
            Ok(())
        } else {
            let mut err = ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Session);
            err.delivery = zero_platform_tokio::contract::DeliveryState::NotSubmitted;
            Err(err)
        }
    }
    fn select(
        &self,
        peer: SocketAddr,
        expected: NetworkGeneration,
    ) -> Result<zero_platform_tokio::EgressSelection, ChannelError> {
        self.check_current(expected)?;
        let mut policy = self.policy.clone();
        if self.drifted.load(std::sync::atomic::Ordering::Acquire) {
            policy.source_ip = Some(std::net::IpAddr::from([127, 0, 0, 1]));
        }
        self.egress
            .select_for_peer_with_policy(peer, &policy)
            .map_err(|source| {
                ChannelError::new(Stage::Prepare, Reason::Io, Impact::Session).with_source(source)
            })
    }
    fn note_stale_selection(&self, expected: NetworkGeneration) -> bool {
        self.egress.invalidate_generation(expected.0)
    }
}

#[tokio::test]
async fn same_generation_selection_drift_reports_diagnostic_unsubmitted_error_and_invalidates_network_once(
) {
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = peer.local_addr().unwrap();
    let stop = scope.registrar().cancellation();
    let (task, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            stop.cancelled().await;
            Ok(())
        })
        .unwrap();
    let provider =
        HostBoundDatagramResources::new(instance(), task, &declaration(), CancellationToken::new())
            .unwrap();
    provider.activate(task).unwrap();
    let egress = EgressInterfaceControl::default();
    assert_eq!(egress.generation(), 0);
    let auth = Arc::new(DriftPolicy {
        policy: DialPolicy::default(),
        egress: egress.clone(),
        drifted: std::sync::atomic::AtomicBool::new(false),
    });
    let binding = provider
        .authorize(auth.clone(), NetworkGeneration(0))
        .unwrap();
    let grant = provider
        .open_bound_outbound_datagram(BoundDatagramRequest {
            binding,
            peer: address,
            expected_network_generation: NetworkGeneration(0),
        })
        .unwrap();

    auth.drifted
        .store(true, std::sync::atomic::Ordering::Release);
    assert_eq!(egress.generation(), 0);

    for attempt in 0..3 {
        let err = grant
            .socket
            .send_to_addr(b"stale-packet", address)
            .await
            .expect_err("drifted outer UDP binding must reject send before submission");
        let channel = err
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<ChannelError>())
            .expect("typed ChannelError preserved");
        assert_eq!(channel.stage, Stage::Send);
        assert_eq!(channel.reason, Reason::StalePlan);
        assert_eq!(
            channel.delivery,
            zero_platform_tokio::contract::DeliveryState::NotSubmitted
        );
        if attempt == 0 {
            let detail = channel
                .source()
                .expect("selection mismatch source attached")
                .to_string();
            assert!(
                detail.contains("bound UDP egress selection mismatch")
                    && detail.contains("dial_source"),
                "unexpected mismatch diagnostic: {detail}"
            );
        }
        assert_eq!(
            egress.generation(),
            1,
            "same-generation drift burst must invalidate the host network exactly once"
        );
    }

    let returned = grant.socket.return_observation();
    let receipt = grant.socket.close().unwrap();
    assert_eq!(receipt.reservation(), grant.socket.reservation());
    drop(grant);
    assert!(returned.is_returned());
    let report = scope
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await;
    assert!(report.is_complete());
    assert!(witness.is_joined());
    assert!(provider.shutdown_io().unwrap().is_complete());
}
