//! Preserve the existing pre-payload root-candidate retry boundary.
use super::*;
use zero_platform_tokio::controlled_carrier::{CarrierCandidates, CarrierGrant};
pub(in super::super) enum Opened {
    Ready(Prepared),
    CandidateFailure(io::Error),
}
pub(in super::super) struct Candidate {
    pub candidates: Arc<CarrierCandidates>,
    pub index: usize,
    pub udp: bool,
    pub username: String,
    pub password: String,
    pub options: mieru_config::MieruTransportOptions,
    pub end: Instant,
}
impl Candidate {
    pub(in super::super) async fn open(
        self,
        lease: &CarrierLease,
        adoption: &CarrierAdoption,
        grant: CarrierGrant,
    ) -> io::Result<Opened> {
        let stop = grant.cancellation();
        run(lease, adoption, stop, move |_| async move {
            if self.udp {
                let io = match self.candidates.prepare_datagram(self.index, grant) {
                    Ok(io) => io,
                    Err(source) => return Ok(Opened::CandidateFailure(io::Error::other(source))),
                };
                let ipv6 = io.peer().is_ipv6();
                ClientConnection::prepare_packet_driver(
                    io.into_driver(),
                    ipv6,
                    &self.username,
                    &self.password,
                    &self.options,
                )
                .map(Prepared::Packet)
                .map(Opened::Ready)
            } else {
                let open = async {
                    self.candidates
                        .prepare_stream(self.index, grant)?
                        .open(self.end)
                        .await
                };
                let io = match open.await {
                    Ok(ready) => ready.into_parts().0,
                    Err(source) => return Ok(Opened::CandidateFailure(io::Error::other(source))),
                };
                ClientConnection::prepare_tcp_controlled(
                    io,
                    &self.username,
                    &self.password,
                    &self.options,
                )
                .await
                .map(Prepared::Tcp)
                .map(Opened::Ready)
            }
        })
        .await
    }
}
