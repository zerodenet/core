use super::*;
pub(in crate::component::quic) enum Ready {
    Stream(zero_platform_tokio::TcpRelayStream),
    H3(zero_transport::split_http::OwnedXhttp3),
}
pub(in crate::component::quic) struct Pending(pub Option<PacketMetadataArc<Entry>>);
impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(entry) = &self.0 {
            entry.retirement.request_retirement();
        }
    }
}
pub(in crate::component::quic) struct Initialized {
    pub entry: PacketMetadataArc<Entry>,
    pub io: Option<Ready>,
    pub(super) pending: Pending,
}
impl Initialized {
    pub fn into_relay(mut self) -> zero_platform_tokio::TcpRelayStream {
        let Some(Ready::Stream(io)) = self.io.take() else {
            panic!("plain stream path requires its real bi-stream");
        };
        let control = zero_traits::AsyncSocket::transport_bypass_control(&io);
        let view = entry::View {
            io: Some(io),
            original: self.entry.clone(),
        };
        let relay = match control {
            Some(control) => {
                zero_platform_tokio::TcpRelayStream::with_transport_bypass_control(view, control)
            }
            None => zero_platform_tokio::TcpRelayStream::new(view),
        };
        self.pending.0.take();
        relay
    }
}
impl Initialized {
    pub fn into_h3(mut self) -> zero_transport::split_http::OwnedXhttp3 {
        let Some(Ready::H3(client)) = self.io.take() else {
            panic!("H3 pool requires the actual ready H3 sender");
        };
        self.pending.0.take();
        client
    }
}
