//! Authenticate one UDP peer and transfer its protocol session driver to runtime.
use super::{multiplex::MieruInboundMultiplexer, MieruInboundProfile};
use crate::{
    metadata::OPEN_SESSION_REQUEST,
    packet::{Driver, PacketCodec, PacketIo},
};
use std::{
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::UdpSocket, sync::mpsc};

impl MieruInboundProfile {
    pub async fn accept_packet_peer(
        &self,
        socket: Arc<UdpSocket>,
        peer: SocketAddr,
        packets: mpsc::Receiver<Vec<u8>>,
    ) -> io::Result<MieruInboundMultiplexer> {
        let (mut connection, driver) = self
            .prepare_packet_io(
                PacketIo::Server {
                    socket,
                    peer,
                    packets,
                },
                peer,
            )
            .await?;
        connection.install_compatibility_driver(tokio::spawn(async move {
            let _ = driver.await;
        }));
        Ok(connection)
    }
    pub(crate) async fn prepare_packet_peer(
        &self,
        peer: zero_platform_tokio::contract::inbound::udp::DatagramPeer,
    ) -> io::Result<(
        MieruInboundMultiplexer,
        impl std::future::Future<Output = io::Result<()>> + Send + 'static,
    )> {
        let remote = peer
            .connection_facts()?
            .effective_source
            .ok_or(io::ErrorKind::NotConnected)?;
        let (tx, rx, _) = peer.split();
        self.prepare_packet_io(PacketIo::Hosted { tx, rx }, remote)
            .await
    }
    async fn prepare_packet_io(
        &self,
        mut io: PacketIo,
        peer: SocketAddr,
    ) -> io::Result<(
        MieruInboundMultiplexer,
        impl std::future::Future<Output = io::Result<()>> + Send + 'static,
    )> {
        self.options.validate().map_err(io::Error::other)?;
        let authenticate = async {
            loop {
                let packet = io.receive().await?;
                if packet.len() < 72 {
                    continue;
                }
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(io::Error::other)?
                    .as_secs();
                for user in &self.users {
                    for key in crate::crypto::try_derive_keys(&user.username, &user.password, now) {
                        let codec = PacketCodec::configured(
                            key,
                            &user.username,
                            &self.options,
                            peer.is_ipv6(),
                        )?;
                        let Ok(segment) = codec.decode(&packet) else {
                            continue;
                        };
                        let Some(meta) = &segment.session_meta else {
                            continue;
                        };
                        if meta.protocol_type != OPEN_SESSION_REQUEST
                            || meta.session_id == 0
                            || meta.sequence_number != 0
                            || !codec.accept_open(&packet)
                        {
                            continue;
                        }
                        return Ok::<_, io::Error>((codec, user.auth(), packet));
                    }
                }
            }
        };
        let (codec, auth, first) = tokio::time::timeout(Duration::from_secs(5), authenticate)
            .await
            .map_err(|_| io::ErrorKind::TimedOut)??;
        let (ready, incoming) = mpsc::channel(64);
        let (outgoing, commands) = mpsc::channel(64);
        let (dropped, drop_events) = mpsc::unbounded_channel();
        let (_opens, requests) = mpsc::channel(1);
        let book = super::multiplex::reader::Reader::new(
            ready.clone(),
            outgoing,
            dropped,
            self.options.receive.clone(),
        );
        let mut driver = Driver::new(book, codec.clone(), io, false);
        driver.receive(codec.decode(&first)?).await?;
        let failure = Arc::new(Mutex::new(None));
        let result = failure.clone();
        let driver = async move {
            let outcome = crate::packet::run(driver, commands, requests, drop_events).await;
            if let Err(error) = &outcome {
                *result.lock().unwrap() = Some(error.to_string());
            }
            drop(ready);
            outcome
        };
        Ok((
            MieruInboundMultiplexer::from_driver(auth, incoming, failure),
            driver,
        ))
    }
}
