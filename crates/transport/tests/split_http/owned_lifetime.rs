//! The real buffered network endpoint outlives a cancelled logical caller.
use super::*;
use std::num::NonZeroUsize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::controlled_carrier::{CarrierBufferLimits, CarrierOriginalTotals};
#[tokio::test]
async fn cancelled_logical_keeps_actual_duplex_bytes_and_lifetime_header_until_original_work_returns(
) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let root = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
        let memory = root.driver_metadata_view();
        let baseline = memory.snapshot().occupied;
        let original = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        original
            .install_buffer_limits_after_retirement(CarrierBufferLimits {
                max_queue_packets: 1,
                total_max_bytes: 2 * 64 * 1024,
            })
            .unwrap();
        let book = original.buffer_budget();
        let life = crate::split_http::io::LifetimeRef::prepare(&memory).unwrap();
        let (mut stream, mut network) =
            crate::split_http::client::stream_pair_with_lifetime(life.clone());
        let returned = OwnedLogicalReturn::prepare(
            &memory,
            life.clone(),
            book.try_reserve(2 * 64 * 1024).unwrap(),
        )
        .unwrap();
        let work = returned.enter().unwrap();
        stream.owned_return = Some(returned.clone());
        stream
            .write_all(b"still buffered in the actual network endpoint")
            .await
            .unwrap();
        drop(stream);
        drop(life);
        assert!(
            !returned.is_returned(),
            "the actual original work still owns its network endpoint"
        );
        assert_eq!(book.used_bytes(), 2 * 64 * 1024);
        let mut bytes = Vec::new();
        network.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"still buffered in the actual network endpoint");
        drop(network);
        drop(work);
        assert!(returned.is_returned());
        assert_eq!(
            book.used_bytes(),
            2 * 64 * 1024,
            "the other exact endpoint receipt still pins its original storage"
        );
        drop(returned);
        assert_eq!(book.used_bytes(), 0);
        assert_eq!(memory.snapshot().occupied, baseline);
    })
    .await
    .unwrap();
}
