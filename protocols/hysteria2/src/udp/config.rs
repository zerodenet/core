//! UDP connection configuration and packet-path cache identity.
use super::{
    udp_flow_codec, Hysteria2UdpConnectorProfile, Hysteria2UdpFlowResume,
    Hysteria2UdpPacketPathSpec,
};
use alloc::string::String;
use zero_core::{Address, Error};
use zero_traits::DatagramCodec;

fn udp_cache_key(
    tag: &str,
    server: &str,
    port: u16,
    password: &str,
    client_fingerprint: Option<&str>,
) -> String {
    alloc::format!(
        "hysteria2|{}:{tag}|{}:{server}:{port}|{}:{password}|fp:{client_fingerprint:?}",
        tag.len(),
        server.len(),
        password.len()
    )
}

pub struct Hysteria2UdpFlowConfig<'a> {
    tag: &'a str,
    server: &'a str,
    port: u16,
    password: &'a str,
    client_fingerprint: Option<&'a str>,
    insecure: bool,
}

impl<'a> Hysteria2UdpFlowConfig<'a> {
    pub fn with_insecure(mut self, insecure: bool) -> Self {
        self.insecure = insecure;
        self
    }

    pub fn new(
        tag: &'a str,
        server: &'a str,
        port: u16,
        password: &'a str,
        client_fingerprint: Option<&'a str>,
    ) -> Self {
        Self {
            tag,
            server,
            port,
            password,
            client_fingerprint,
            insecure: false,
        }
    }

    pub fn cache_key(&self) -> String {
        alloc::format!(
            "{}|insecure:{}",
            udp_cache_key(
                self.tag,
                self.server,
                self.port,
                self.password,
                self.client_fingerprint
            ),
            self.insecure
        )
    }

    pub fn flow_resume(&self) -> Hysteria2UdpFlowResume {
        Hysteria2UdpFlowResume::new(self.password, self.client_fingerprint)
            .with_insecure(self.insecure)
    }

    pub fn connector_profile(&self) -> Hysteria2UdpConnectorProfile {
        self.flow_resume().connector_profile()
    }

    pub fn packet_path_spec(&self) -> Hysteria2UdpPacketPathSpec {
        Hysteria2UdpPacketPathSpec {
            cache_key: self.cache_key(),
            resume: self.flow_resume(),
        }
    }

    pub fn codec(&self) -> impl DatagramCodec<Address, Error = Error> {
        udp_flow_codec()
    }
}
