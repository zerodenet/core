use super::{
    stream::{connection, Input},
    MkcpStream, Settings,
};
use std::io;
pub async fn connect(
    server: &str,
    port: u16,
    settings: Settings,
    sockets: &crate::OutboundDatagramSocketFactory,
) -> io::Result<MkcpStream> {
    settings.validate()?;
    if sockets.is_relay()
        && sockets
            .final_mask()
            .udp()
            .iter()
            .any(|mask| matches!(mask, crate::finalmask::udp::Mask::Xicmp { .. }))
    {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "raw ICMP cannot bypass a datagram relay",
        ));
    }
    let addresses = sockets.resolve_server_addresses(server, port).await?;
    let mut last = None;
    for peer in addresses {
        match sockets.open_socket(peer).await.and_then(|socket| {
            crate::finalmask::Socket::wrap_with_egress(
                socket,
                sockets.final_mask().udp(),
                false,
                sockets.egress_for(peer).as_ref(),
            )
        }) {
            Ok(socket) => {
                let socket = crate::finalmask::packet_socket::from_socket(socket);
                let (stream, driver) = connection(
                    socket.clone(),
                    peer,
                    rand::random(),
                    settings,
                    Input::Socket(socket, peer),
                )?;
                tokio::spawn(async move {
                    if let Err(error) = driver.await {
                        tracing::debug!(%error,"mKCP carrier ended");
                    }
                });
                return Ok(stream);
            }
            Err(error) => last = Some(error),
        }
    }
    Err(last.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "mKCP endpoint resolved to no addresses",
        )
    }))
}
