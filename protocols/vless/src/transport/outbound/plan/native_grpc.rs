use super::*;
impl OwnedVlessOutboundTransportPlan {
    pub(in crate::transport) fn native_grpc_options(
        &self,
        runtime: &super::super::super::runtime::VlessTransportRuntime,
        dial: zero_traits::DialPolicy,
        carriers: std::num::NonZeroUsize,
        streams: std::num::NonZeroUsize,
    ) -> Result<zero_transport::grpc::NativePoolOptions, RuntimeError> {
        if self.browser_dialer_settings.is_some()
            || self.hysteria.is_some()
            || self.mkcp.is_some()
            || !self.final_mask.tcp().is_empty()
            || !self.final_mask.udp().is_empty()
            || self.encryption.is_some()
            || self.download.is_some()
            || self.transport.reality.is_some()
            || self.transport.ws.is_some()
            || self.transport.h2.is_some()
            || self.transport.http_upgrade.is_some()
            || self.transport.split_http.is_some()
            || self.transport.quic.is_some()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "native gRPC recipe requires an ordinary direct carrier",
            )
            .into());
        }
        let (Some(pool), Some(grpc)) = (&self.grpc_pool, &self.transport.grpc) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "leaf has no actual shared gRPC pool",
            )
            .into());
        };
        if !runtime.owns_grpc_pool(pool) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "gRPC pool is not retained by the actual runtime owner",
            )
            .into());
        }
        // share_xhttp_pool already selected this exact pool using the complete
        // original key. Never format a second key or allocate a parallel pool.
        zero_transport::grpc::NativePoolOptions::from_pool(
            pool,
            zero_transport::grpc::NativePoolSettings {
                server: self.server.clone(),
                port: self.port,
                grpc: grpc.clone(),
                tls: self.transport.tls.clone(),
                dial,
            },
            carriers,
            streams,
        )
    }
}
