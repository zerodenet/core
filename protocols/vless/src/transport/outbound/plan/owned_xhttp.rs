//! Values from the accepted recipe's actual upload/download pools and materials.
//! Neither this projection nor transport kind is an IO/Role/Scope permission.
use super::*;
pub(crate) enum OwnedXhttpKind {
    Http1,
    Http2,
    Http3(VlessQuicClientProfile),
}
pub(crate) struct OwnedXhttpHalf {
    pub plan: OwnedVlessOutboundTransportPlan,
    pub endpoint: zero_core::request::Destination,
    pub kind: OwnedXhttpKind,
    pub tls: Option<OwnedClientTlsProfile>,
    pub reality: Option<VlessRealityClientProfile>,
    pub pool: split_http::XhttpClientPool,
    pub profile: zero_transport::profile::OwnedSplitHttpProfile,
}
pub(crate) struct OwnedXhttpParts {
    pub upload: OwnedXhttpHalf,
    pub download: Option<OwnedXhttpHalf>,
}
impl OwnedVlessOutboundTransportPlan {
    pub(in crate::transport) fn owned_xhttp_parts(&self) -> Option<OwnedXhttpParts> {
        // These earlier real selections retain their own owning factories.
        if self.browser_ws
            || self.browser_xhttp
            || self.grpc_pool.is_some()
            || self.hysteria.is_some()
            || self.mkcp.is_some()
        {
            return None;
        }
        let mut upload = self.owned_xhttp_half()?;
        let mode = if self.download.is_some()
            && self.transport.reality.is_some()
            && split_http::XhttpMode::parse(&upload.profile.mode) == split_http::XhttpMode::Auto
        {
            split_http::XhttpMode::StreamUp
        } else {
            super::super::xhttp::mode(self.stream_transport_options())
        };
        upload.profile.mode = mode.as_str().to_owned();
        let download = match &self.download {
            Some(download) => Some(download.owned_xhttp_half()?),
            None => None,
        };
        Some(OwnedXhttpParts { upload, download })
    }
    fn owned_xhttp_half(&self) -> Option<OwnedXhttpHalf> {
        if self.browser_ws
            || self.browser_xhttp
            || self.grpc_pool.is_some()
            || self.hysteria.is_some()
            || self.mkcp.is_some()
        {
            return None;
        }
        let profile = self.transport.split_http.clone()?;
        let pool = self.xhttp_pool.clone()?;
        let kind = match self.transport.quic.clone() {
            Some(profile) => OwnedXhttpKind::Http3(profile),
            None if self.transport.tls.is_some() || self.transport.reality.is_some() => {
                OwnedXhttpKind::Http2
            }
            None => OwnedXhttpKind::Http1,
        };
        let address = zero_core::address::parse_ip_literal(&self.server)
            .map(|ip| match ip {
                std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
                std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
            })
            .unwrap_or_else(|| zero_core::Address::Domain(self.server.clone()));
        let endpoint = zero_core::request::Destination::new(address, self.port);
        let mut plan = self.clone();
        // The existing pool is transferred separately; a carrier's recipe must
        // not retain its own pool or recursively establish a second download.
        plan.xhttp_pool = None;
        plan.download = None;
        plan.encryption = None;
        Some(OwnedXhttpHalf {
            plan,
            endpoint,
            kind,
            // The old XHTTP carrier selects REALITY before TLS. An ignored
            // adjacent TLS option must not introduce a fresh CA File read.
            tls: if self.transport.reality.is_some() {
                None
            } else {
                self.transport.tls.clone().map(|mut tls| {
                    tls.alpn = vec!["h2".into()];
                    tls
                })
            },
            reality: self.transport.reality.clone(),
            pool,
            profile,
        })
    }
}

impl OwnedVlessOutboundTransportPlan {
    pub(crate) fn final_mask_profile(&self) -> &zero_transport::finalmask::Profile {
        &self.final_mask
    }
}

impl OwnedVlessOutboundTransportPlan {
    pub(in crate::transport) fn share_owned_mask_pools(
        &mut self,
        runtime: &crate::transport::VlessTransportRuntime,
        tag: &str,
        identity: [u8; 32],
    ) {
        // Same Debug bytes as the original Profile. This is only the actual
        // immutable pool key; it is never a compiled-mask/IO/Scope proof.
        struct Identity([u8; 32]);
        impl std::fmt::Debug for Identity {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_tuple("FinalMaskProfile").field(&self.0).finish()
            }
        }
        if self.transport.grpc.is_some() {
            self.grpc_pool = Some(runtime.grpc_pool(
                tag,
                &format!(
                    "{:?}",
                    (&self.server, self.port, &self.transport, Identity(identity))
                ),
            ));
        }
        if let Some(profile) = &self.transport.split_http {
            self.xhttp_pool = Some(runtime.xhttp_pool(
                tag,
                &format!(
                    "{:?}",
                    (&self.server, self.port, &self.transport, Identity(identity))
                ),
                profile.options.xmux,
            ));
        }
        // The independent download keeps its original unmasked recipe/key.
    }
}
