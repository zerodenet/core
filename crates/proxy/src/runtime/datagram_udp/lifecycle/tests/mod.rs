#![cfg(feature = "socks5")]

use zero_core::{Address, DatagramUdpResponder, InboundUdpDispatch};

use super::relay::DatagramUdpLoopContext;
use super::with_upstream;

mod fixtures;
#[cfg(feature = "managed-stream-runtime")]
mod packet_session;

use fixtures::{UpstreamFixture, IDLE_TIMEOUT};

struct PendingResponder {
    reads: usize,
}

#[async_trait::async_trait]
impl DatagramUdpResponder<()> for PendingResponder {
    async fn read_inbound_dispatch(
        &mut self,
        _source: &(),
    ) -> Result<Option<InboundUdpDispatch>, zero_core::Error> {
        self.reads += 1;
        assert!(self.reads < 32, "expired upstream timer caused a busy loop");
        std::future::pending().await
    }

    async fn write_response_for_session(
        &mut self,
        _source: &(),
        _session_id: Option<u64>,
        _target: &Address,
        _port: u16,
        _payload: &[u8],
    ) -> Result<Option<usize>, zero_core::Error> {
        panic!("upstream sends no responses in idle reclamation test")
    }
}

#[tokio::test]
async fn datagram_loop_reclaims_idle_upstream_without_new_traffic() {
    let (fixture, mut dispatch) = UpstreamFixture::establish().await;
    // Complete real socket setup before freezing time. The loop is polled
    // explicitly so an expired no-op timer cannot strand the test task.
    tokio::time::pause();
    dispatch.touch_upstream_idle(IDLE_TIMEOUT);
    let idle_deadline = dispatch.poll_refs().2.expect("upstream idle deadline");
    let context = DatagramUdpLoopContext {
        runtime: &fixture.runtime,
        auth: None,
    };
    let mut responder = PendingResponder { reads: 0 };
    let mut direct_buf = [0; 1024];
    let mut upstream_buf = [0; 1024];
    {
        let relay = with_upstream::run_loop(
            &context,
            &(),
            &mut responder,
            &mut dispatch,
            &mut direct_buf,
            &mut upstream_buf,
        );
        tokio::pin!(relay);
        assert!(futures_util::poll!(relay.as_mut()).is_pending());
        assert_eq!(fixture.proxy.stats_snapshot().udp_upstream.idle_timeouts, 0);
        // Await the same deadline with time paused. Unlike advance(), this
        // waits for the timer driver to process its millisecond-rounded tick.
        tokio::time::sleep_until(idle_deadline).await;
        assert!(futures_util::poll!(relay.as_mut()).is_pending());
        fixture.assert_idle_closed();
        assert!(futures_util::poll!(relay.as_mut()).is_pending());
        fixture.assert_idle_closed();
    }
    tokio::time::resume();
    fixture.assert_reestablishes(dispatch).await;
}
