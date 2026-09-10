use super::MieruTransportLeaf;
use crate::{
    client::{ClientConnection, ClientPool, PoolKey},
    inbound::multiplex::stream::MieruLogicalStream,
};
use std::{future::Future, sync::Arc};
use zero_platform_tokio::TokioSocket;
use zero_transport::{OutboundDatagramSocketFactory, RuntimeError};
impl MieruTransportLeaf {
    pub fn with_pool(mut self, pool: Arc<ClientPool>) -> Self {
        self.pool = pool;
        self
    }
    pub fn with_udp(mut self, udp: bool) -> Self {
        self.udp = udp;
        self
    }
    pub(super) fn cache_identity(&self) -> String {
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        for value in [&self.tag, &self.server, &self.username, &self.password] {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
        hash.update(format!("{:?}", self.options).as_bytes());
        hash.update(self.port.to_be_bytes());
        hash.update([u8::from(self.udp)]);
        format!("{:x}", hash.finalize())
    }
    pub(super) async fn open_pooled<F, Fut, E>(
        &self,
        open_socket: F,
        factory: OutboundDatagramSocketFactory,
    ) -> Result<MieruLogicalStream, RuntimeError>
    where
        F: Clone + Fn(&str, u16) -> Fut + Send + Sync,
        Fut: Future<Output = Result<TokioSocket, E>> + Send,
        E: Into<RuntimeError>,
    {
        let key = PoolKey::new(
            &self.tag,
            &self.server,
            self.port,
            &self.username,
            &self.password,
            self.udp,
        )
        .with_generation(factory.egress_generation())
        .with_profile(format!("direct:{}", self.cache_identity()));
        self.pool
            .open(key, || async {
                if self.udp {
                    let addresses = factory
                        .resolve_server_addresses(&self.server, self.port)
                        .await?;
                    let mut last = std::io::Error::other("mieru endpoint unavailable");
                    for peer in addresses {
                        match factory.bind_std(peer).and_then(|socket| {
                            socket.set_nonblocking(true)?;
                            tokio::net::UdpSocket::from_std(socket)
                        }) {
                            Ok(socket) => {
                                match ClientConnection::udp_with_options(
                                    Arc::new(socket),
                                    peer,
                                    &self.username,
                                    &self.password,
                                    &self.options,
                                )
                                .await
                                {
                                    Ok(connection) => return Ok(connection),
                                    Err(error) => last = error,
                                }
                            }
                            Err(error) => last = error,
                        }
                    }
                    Err(last)
                } else {
                    let socket = open_socket(&self.server, self.port).await.map_err(|e| {
                        let e: RuntimeError = e.into();
                        std::io::Error::other(e.to_string())
                    })?;
                    ClientConnection::tcp_with_options(
                        socket,
                        &self.username,
                        &self.password,
                        &self.options,
                    )
                    .await
                }
            })
            .await
            .map_err(RuntimeError::Io)
    }

    pub(super) async fn open_pooled_relay_tcp<F, Fut, E>(
        &self,
        generation: u64,
        relay_identity: &str,
        mut open_carrier: F,
    ) -> Result<MieruLogicalStream, RuntimeError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<zero_platform_tokio::TcpRelayStream, E>> + Send,
        E: Into<RuntimeError>,
    {
        let key = self.pool_key(generation).with_profile(format!(
            "tcp-relay:{relay_identity}:{}",
            self.cache_identity()
        ));
        let username = self.username.clone();
        let password = self.password.clone();
        let options = self.options.clone();
        self.pool
            .open(key, || {
                let carrier = open_carrier();
                let username = username.clone();
                let password = password.clone();
                let options = options.clone();
                async move {
                    let stream = carrier.await.map_err(|error| {
                        let error: RuntimeError = error.into();
                        std::io::Error::other(error.to_string())
                    })?;
                    ClientConnection::tcp_with_options(stream, &username, &password, &options).await
                }
            })
            .await
            .map_err(RuntimeError::Io)
    }

    pub(super) async fn open_pooled_relay_datagram<F, Fut, E>(
        &self,
        generation: u64,
        relay_identity: &str,
        mut open_carrier: F,
    ) -> Result<MieruLogicalStream, RuntimeError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<Arc<dyn crate::client::ClientDatagramCarrier>, E>> + Send,
        E: Into<RuntimeError>,
    {
        let key = self.pool_key(generation).with_profile(format!(
            "datagram-relay:{relay_identity}:{}",
            self.cache_identity()
        ));
        let username = self.username.clone();
        let password = self.password.clone();
        let options = self.options.clone();
        self.pool
            .open(key, || {
                let carrier = open_carrier();
                let username = username.clone();
                let password = password.clone();
                let options = options.clone();
                async move {
                    let carrier = carrier.await.map_err(|error| {
                        let error: RuntimeError = error.into();
                        std::io::Error::other(error.to_string())
                    })?;
                    ClientConnection::datagram_with_options(
                        carrier, true, &username, &password, &options,
                    )
                    .await
                }
            })
            .await
            .map_err(RuntimeError::Io)
    }

    fn pool_key(&self, generation: u64) -> PoolKey {
        PoolKey::new(
            &self.tag,
            &self.server,
            self.port,
            &self.username,
            &self.password,
            self.udp,
        )
        .with_generation(generation)
    }
}
