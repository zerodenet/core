use core::future::Future;

use tokio::io::{AsyncRead, AsyncWrite};
use zero_core::{InboundStreamRoute, Session};
use zero_traits::AsyncSocket;

use super::{MieruInboundAcceptedSession, MieruInboundUdpRelay};

#[async_trait::async_trait]
impl<S> InboundStreamRoute for MieruInboundAcceptedSession<S>
where
    S: AsyncSocket + AsyncRead + AsyncWrite + 'static,
{
    type TcpStream = S;
    type UdpRelay = MieruInboundUdpRelay<S>;

    async fn dispatch_inbound_route<E, FTcp, FTcpFut, FUdp, FUdpFut>(
        self,
        on_tcp: FTcp,
        on_udp: FUdp,
    ) -> Result<(), E>
    where
        FTcp: FnOnce(Session, S) -> FTcpFut + Send,
        FTcpFut: Future<Output = Result<(), E>> + Send,
        FUdp: FnOnce(Session, Self::UdpRelay) -> FUdpFut + Send,
        FUdpFut: Future<Output = Result<(), E>> + Send,
    {
        match self {
            Self::Tcp { session, stream } => on_tcp(session, stream).await,
            Self::Udp { session, relay } => on_udp(session, relay).await,
        }
    }
}
