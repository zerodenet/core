//! Quinn borrows the actual logical packet loan; its donor retains native custody.
use super::*;
use zero_platform_tokio::{
    contract::{lifecycle::CloseReport, CloseReason},
    controlled_carrier::loan::{DatagramLoan, LoanOwner},
};
pub(super) struct BorrowedSocket {
    pub(super) socket: Arc<dyn quinn::AsyncUdpSocket>,
    pub(super) loan: LoanOwner,
    pub(super) driver: Option<crate::datagram_driver::PreparedIoDriver>,
    pub(super) stop: zero_platform_tokio::contract::CancellationToken,
    pub(super) callbacks: Arc<super::super::callbacks::Broker>,
}
impl OwnedEndpointOwner {
    /// Cold composition. Keep this same owner before start: the actual loan
    /// controller survives interruption, independent of queue and Quinn tasks.
    pub fn new_borrowed(
        loan: DatagramLoan,
        peer: std::net::SocketAddr,
        registrar: TaskRegistrar,
        limits: OwnedEndpointLimits,
    ) -> Result<(Self, DriverExitReceiver), (io::Error, DatagramLoan)> {
        let callbacks = match super::super::callbacks::Broker::new(1) {
            Ok(callbacks) => callbacks,
            Err(error) => return Err((error, loan)),
        };
        let DatagramLoan { io, owner: loan } = loan;
        let local = std::net::SocketAddr::new(
            if peer.is_ipv4() {
                std::net::Ipv4Addr::UNSPECIFIED.into()
            } else {
                std::net::Ipv6Addr::UNSPECIFIED.into()
            },
            0,
        );
        // Preparation is inert and cannot fail after taking this real loan.
        let (socket, driver) = crate::datagram_queue::prepare_with(
            local,
            Arc::new(move |packet| {
                if packet.destination != peer {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "borrowed Quinn carrier changed destination",
                    ));
                }
                Ok(packet.contents.to_vec())
            }),
            move |mut channels| async move {
                let sending = async {
                    while let Some(packet) = channels.outgoing.recv().await {
                        io.send(&packet.bytes).await?;
                    }
                    Ok::<_, io::Error>(())
                };
                let receiving = async {
                    let mut bytes = vec![0; 65536];
                    loop {
                        let count = io.receive(&mut bytes).await?;
                        if channels
                            .incoming
                            .send(crate::datagram_queue::Packet {
                                bytes: bytes[..count].to_vec(),
                                peer,
                            })
                            .await
                            .is_err()
                        {
                            return Ok::<_, io::Error>(());
                        }
                    }
                };
                tokio::select! {result=sending=>result,result=receiving=>result}
            },
        )
        .expect("inert bounded packet queue preparation");
        let stop = driver.cancel.clone();
        Ok(Self::construct(
            Native::Borrowed(BorrowedSocket {
                socket,
                loan,
                driver: Some(driver),
                stop,
                callbacks,
            }),
            registrar,
            limits,
        ))
    }
    /// Sealed endpoint users and actual driver receipts must have completed
    /// before the original loan's controller can confirm logical return.
    /// This never asserts the donor's native originals have closed.
    pub async fn shutdown_borrowed(
        &mut self,
        deadline: Instant,
    ) -> Result<CloseReport, ChannelError> {
        let users = self.shutdown_users(deadline).await?;
        if !users.users_and_drivers_complete() {
            return Ok(CloseReport {
                unfinished: vec![
                    zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                        "borrowed Quinn original users or driver receipts".into(),
                    ),
                ],
                ..Default::default()
            });
        }
        let Native::Borrowed(native) = &mut self.native else {
            return Err(error(Stage::Close, Reason::Unsupported));
        };
        // An unstarted worker still contains the unique loan I/O view.
        native.stop.cancel();
        drop(native.driver.take());
        native.loan.cancel(CloseReason::Shutdown);
        native.loan.close(deadline).await
    }
}
