use tokio::sync::oneshot;
use zero_core::Address;

use super::fixtures::{UpstreamFixture, IDLE_TIMEOUT};
use crate::runtime::packet_session_udp::{
    run_packet_session_udp_relay_with_dispatch, PacketSessionUdpFailurePolicy,
    PacketSessionUdpHandler, PacketSessionUdpLoopExit, PacketSessionUdpReadFailure,
    PacketSessionUdpReadResult, PacketSessionUdpRelayRequest,
};

struct PendingHandler {
    reads: usize,
    finish: oneshot::Receiver<()>,
}

impl PacketSessionUdpHandler for PendingHandler {
    async fn read_inbound_dispatch(
        &mut self,
    ) -> Result<PacketSessionUdpReadResult, PacketSessionUdpReadFailure> {
        self.reads += 1;
        assert!(self.reads < 32, "expired upstream timer caused a busy loop");
        let _ = (&mut self.finish).await;
        Ok(PacketSessionUdpReadResult::End)
    }

    async fn write_response_for_target(
        &mut self,
        _target: &Address,
        _port: u16,
        _payload: &[u8],
    ) -> Result<usize, zero_core::Error> {
        panic!("upstream sends no responses in idle reclamation test")
    }
}

#[tokio::test]
async fn packet_session_loop_reclaims_idle_upstream_before_session_timeout() {
    let (fixture, mut dispatch) = UpstreamFixture::establish().await;
    tokio::time::pause();
    dispatch.touch_upstream_idle(IDLE_TIMEOUT);
    let idle_deadline = dispatch.poll_refs().2.expect("upstream idle deadline");
    let (finish, finish_rx) = oneshot::channel();
    let relay = run_packet_session_udp_relay_with_dispatch(
        fixture.runtime.clone(),
        PacketSessionUdpRelayRequest {
            handler: PendingHandler {
                reads: 0,
                finish: finish_rx,
            },
            inbound_tag: "idle-test",
            protocol: "idle-test",
            auth: None,
            failure_policy: PacketSessionUdpFailurePolicy::ReturnError,
        },
        dispatch,
    );
    tokio::pin!(relay);
    assert!(futures_util::poll!(relay.as_mut()).is_pending());
    assert_eq!(fixture.proxy.stats_snapshot().udp_upstream.idle_timeouts, 0);
    // Synchronize with the timer driver using virtual time, rather than
    // assuming advance() also delivered every timer wake at that instant.
    tokio::time::sleep_until(idle_deadline).await;
    assert!(futures_util::poll!(relay.as_mut()).is_pending());
    fixture.assert_idle_closed();
    assert!(futures_util::poll!(relay.as_mut()).is_pending());
    fixture.assert_idle_closed();

    finish.send(()).unwrap();
    let exit = relay.await;
    assert!(matches!(
        exit.outcome,
        Ok(PacketSessionUdpLoopExit::InboundEnded)
    ));
    tokio::time::resume();
    fixture.assert_reestablishes(exit.dispatch).await;
}
