use super::*;
#[cfg(feature = "crypto")]
impl ShadowsocksInboundUdpSession {
    pub fn new(codec: ShadowsocksInboundUdpCodec) -> Self {
        Self {
            bindings: state::Bindings::with_limits(codec.limits),
            codec,
        }
    }

    pub fn decode_request(
        &mut self,
        datagram: &[u8],
    ) -> Result<ShadowsocksInboundUdpPacket, Error> {
        self.codec.decode_request(datagram)
    }

    pub fn decode_dispatch_parts(
        &mut self,
        datagram: &[u8],
    ) -> Result<ShadowsocksInboundUdpDispatchParts, Error> {
        self.decode_request(datagram)
            .map(ShadowsocksInboundUdpPacket::into_dispatch_parts)
    }

    pub fn decode_inbound_dispatch(
        &mut self,
        datagram: &[u8],
    ) -> Result<InboundUdpDispatch, Error> {
        self.decode_dispatch_parts(datagram)
            .map(ShadowsocksInboundUdpDispatchParts::into_inbound_dispatch)
    }

    pub fn encode_response_to_client(
        &self,
        client_session_id: Option<u64>,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<ShadowsocksInboundUdpResponse, Error> {
        self.codec
            .encode_response_to_client(client_session_id, target, port, payload)
    }

    pub fn response_frame(
        &self,
        target: &ShadowsocksInboundUdpResponseTarget,
        payload: &[u8],
    ) -> Result<ShadowsocksInboundUdpResponse, Error> {
        self.codec.response_frame(target, payload)
    }

    pub fn response_frame_for_proxy_session(
        &self,
        proxy_session_id: u64,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<ShadowsocksInboundUdpResponse, Error> {
        let response_target =
            self.response_target_for_proxy_session(proxy_session_id, target, port);
        self.response_frame(&response_target, payload)
    }

    pub fn response_datagram_for_proxy_session(
        &self,
        proxy_session_id: u64,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<ShadowsocksInboundUdpResponseDatagram, Error> {
        let datagram =
            self.response_frame_for_proxy_session(proxy_session_id, target, port, payload)?;
        Ok(ShadowsocksInboundUdpResponseDatagram {
            datagram: datagram.into_datagram(),
        })
    }

    pub async fn send_response_to_client_tokio(
        &self,
        socket: &tokio::net::UdpSocket,
        proxy_session_id: u64,
        target: &Address,
        port: u16,
        payload: &[u8],
        client: std::net::SocketAddr,
    ) -> Result<usize, Error> {
        let datagram =
            self.response_datagram_for_proxy_session(proxy_session_id, target, port, payload)?;
        socket
            .send_to(datagram.as_slice(), client)
            .await
            .map_err(|_| Error::Io("failed to send Shadowsocks UDP response"))
    }

    pub async fn send_client_response_to_client_tokio(
        &self,
        socket: &tokio::net::UdpSocket,
        proxy_session_id: u64,
        response: ShadowsocksInboundUdpClientResponse<'_>,
        client: std::net::SocketAddr,
    ) -> Result<usize, Error> {
        self.send_response_to_client_tokio(
            socket,
            proxy_session_id,
            response.target(),
            response.port(),
            response.payload(),
            client,
        )
        .await
    }

    pub fn record_dispatch_success(
        &mut self,
        proxy_session_id: u64,
        client_session_id: Option<u64>,
        client: std::net::SocketAddr,
    ) {
        self.bindings
            .record(proxy_session_id, client_session_id, client);
    }

    pub async fn send_proxy_session_response_to_client_tokio(
        &self,
        socket: &tokio::net::UdpSocket,
        proxy_session_id: u64,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Option<usize>, Error> {
        let Some(client) = self.bindings.client(proxy_session_id) else {
            return Ok(None);
        };
        self.send_response_to_client_tokio(socket, proxy_session_id, target, port, payload, client)
            .await
            .map(Some)
    }

    pub async fn send_proxy_session_client_response_to_client_tokio(
        &self,
        socket: &tokio::net::UdpSocket,
        proxy_session_id: u64,
        response: ShadowsocksInboundUdpClientResponse<'_>,
    ) -> Result<Option<usize>, Error> {
        let Some(client) = self.bindings.client(proxy_session_id) else {
            return Ok(None);
        };
        self.send_client_response_to_client_tokio(socket, proxy_session_id, response, client)
            .await
            .map(Some)
    }

    pub async fn send_client_response_for_proxy_session_to_client_tokio(
        &self,
        socket: &tokio::net::UdpSocket,
        proxy_session_id: Option<u64>,
        response: ShadowsocksInboundUdpClientResponse<'_>,
    ) -> Result<Option<usize>, Error> {
        let Some(proxy_session_id) = proxy_session_id else {
            return Ok(None);
        };
        self.send_proxy_session_client_response_to_client_tokio(socket, proxy_session_id, response)
            .await
    }

    pub async fn send_response_for_target_proxy_session_to_client_tokio(
        &self,
        socket: &tokio::net::UdpSocket,
        proxy_session_id: Option<u64>,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Option<usize>, Error> {
        let Some(proxy_session_id) = proxy_session_id else {
            return Ok(None);
        };
        self.send_proxy_session_response_to_client_tokio(
            socket,
            proxy_session_id,
            target,
            port,
            payload,
        )
        .await
    }

    pub fn response_target_for_proxy_session(
        &self,
        proxy_session_id: u64,
        target: &Address,
        port: u16,
    ) -> ShadowsocksInboundUdpResponseTarget {
        ShadowsocksInboundUdpResponseTarget::from_parts(
            self.bindings.session(proxy_session_id),
            target,
            port,
        )
    }
}
