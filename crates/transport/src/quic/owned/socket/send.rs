use super::*;
use zero_platform_tokio::controlled_datagram::metadata::{
    DatagramEcn, MetadataSendErrorOrigin, MetadataTransmit,
};
#[derive(Default)]
pub(super) struct NativeDrops {
    pub(super) count: u64,
    pub(super) overflowed: bool,
    pub(super) first: Option<Arc<io::Error>>,
}
impl MetadataSocket {
    pub(super) fn send(&self, transmit: &quinn::udp::Transmit<'_>) -> io::Result<()> {
        self.callbacks.check()?;
        let message = MetadataTransmit {
            destination: transmit.destination,
            contents: transmit.contents,
            source_ip: transmit.src_ip,
            ecn: transmit.ecn.map(|ecn| match ecn {
                quinn::udp::EcnCodepoint::Ect0 => DatagramEcn::Ect0,
                quinn::udp::EcnCodepoint::Ect1 => DatagramEcn::Ect1,
                quinn::udp::EcnCodepoint::Ce => DatagramEcn::Ce,
            }),
            segment_size: transmit.segment_size,
        };
        match self.sender.try_send_detailed(&message) {
            // A positive partial prefix is consumed once, as in pinned Quinn.
            // Never resubmit the full buffer or count its unsubmitted suffix.
            Ok(_) => Ok(()),
            Err(failure)
                if failure.origin == MetadataSendErrorOrigin::Native
                    && failure.error.kind() != io::ErrorKind::WouldBlock
                    && failure.error.raw_os_error().is_some() =>
            {
                // Pinned Quinn treats actual native transmission errors as
                // nonfatal drops. Keep that behavior and the actual first source;
                // no packet/byte/submission receipt is fabricated here.
                let source = Arc::new(failure.error);
                {
                    let mut drops = self.drops.lock().unwrap();
                    match drops.count.checked_add(1) {
                        Some(count) => drops.count = count,
                        None => drops.overflowed = true,
                    }
                    if drops.first.is_none() {
                        drops.first = Some(source.clone());
                    }
                }
                tracing::warn!(error = %source, "owned Quinn native UDP send dropped");
                Ok(())
            }
            Err(failure) => Err(failure.error),
        }
    }
}
