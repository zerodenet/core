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
        mut packets: mpsc::Receiver<Vec<u8>>,
    ) -> io::Result<MieruInboundMultiplexer> {
        self.options.validate().map_err(io::Error::other)?;
        let authenticate = async {
            loop {
                let packet = packets.recv().await.ok_or(io::ErrorKind::BrokenPipe)?;
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
        let mut driver = Driver::new(
            book,
            codec.clone(),
            PacketIo::Server {
                socket,
                peer,
                packets,
            },
            false,
        );
        driver.receive(codec.decode(&first)?).await?;
        let failure = Arc::new(Mutex::new(None));
        let result = failure.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = crate::packet::run(driver, commands, requests, drop_events).await {
                *result.lock().unwrap() = Some(error.to_string());
            }
            drop(ready);
        });
        Ok(MieruInboundMultiplexer::from_driver(
            auth, incoming, failure, task,
        ))
    }
}
