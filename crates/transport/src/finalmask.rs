//! Xray v26.3.27 FinalMask carrier transforms. No proxy protocol or routing state.
pub mod noise;
pub mod packet_socket;
pub mod sudoku;
pub mod tcp;
pub mod udp;

mod queue;
mod socket;
mod xdns;
mod xicmp;
pub use socket::Socket;
#[derive(Clone)]
pub struct Profile {
    udp: std::sync::Arc<[udp::Mask]>,
    tcp: tcp::PreparedMasks,
    identity: [u8; 32],
}
impl std::fmt::Debug for Profile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("FinalMaskProfile")
            .field(&self.identity())
            .finish()
    }
}
#[derive(Debug, Clone, Default)]
pub struct Settings {
    pub udp: Vec<udp::Mask>,
    pub tcp: Vec<tcp::Mask>,
}
impl Profile {
    pub fn new(settings: Settings) -> std::io::Result<Self> {
        udp::validate(&settings.udp)?;
        let input = format!("{:?}:{:?}", settings.udp, settings.tcp);
        let identity = ring::digest::digest(&ring::digest::SHA256, input.as_bytes())
            .as_ref()
            .try_into()
            .unwrap();
        let tcp = tcp::PreparedMasks::new(&settings.tcp)?;
        Ok(Self {
            udp: settings.udp.into(),
            tcp,
            identity,
        })
    }
    pub fn tcp(&self) -> &tcp::PreparedMasks {
        &self.tcp
    }
    pub fn from_udp(masks: Vec<udp::Mask>) -> std::io::Result<Self> {
        Self::new(Settings {
            udp: masks,
            tcp: Vec::new(),
        })
    }
    pub fn udp(&self) -> &[udp::Mask] {
        &self.udp
    }
    pub fn identity(&self) -> [u8; 32] {
        self.identity
    }
}
impl Default for Profile {
    fn default() -> Self {
        Self::new(Settings::default()).expect("empty FinalMask profile")
    }
}
