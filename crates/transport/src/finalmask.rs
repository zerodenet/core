//! Xray v26.3.27 FinalMask carrier transforms. No proxy protocol or routing state.
pub mod noise;
pub mod packet_socket;
pub mod sudoku;
pub mod tcp;
pub mod udp;

mod material;
mod queue;
mod socket;
mod xdns;
mod xicmp;
pub use socket::Socket;
#[derive(Clone)]
pub struct Profile {
    udp: material::UdpRecipe,
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
        let identity = identity(&settings);
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

pub mod owned;

mod fifo;
pub(crate) mod memory;
mod rows;

fn identity(settings: &Settings) -> [u8; 32] {
    use std::fmt::Write;
    struct Hash(ring::digest::Context);
    impl Write for Hash {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            self.0.update(value.as_bytes());
            Ok(())
        }
    }
    let mut hash = Hash(ring::digest::Context::new(&ring::digest::SHA256));
    write!(&mut hash, "{:?}:{:?}", settings.udp, settings.tcp)
        .expect("infallible original material hash sink");
    hash.0.finish().as_ref().try_into().unwrap()
}
impl Profile {
    /// Kernel construction, from the same accepted settings and Host books.
    /// UDP validation is temporarily prepaid; each actual later owned UDP
    /// codec retains its own material credit. TCP tables are built only once.
    pub fn new_owned(
        settings: Settings,
        book: zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
        metadata: zero_platform_tokio::contract::component::PacketDriverMetadataView,
    ) -> std::io::Result<Self> {
        udp::validate_owned(&settings.udp, &book)?;
        let identity = identity(&settings);
        let tcp = tcp::PreparedMasks::new_owned(&settings.tcp, book.clone(), metadata.clone())?;
        let udp = material::UdpRecipe::new_owned(settings.udp, &book, &metadata)?;
        Ok(Self { udp, tcp, identity })
    }
}
impl Profile {
    /// Repeated accepted-recipe build/recovery retains the original raw
    /// settings. Clone its actual backing only after the same Host pays it.
    pub fn new_owned_from_ref(
        settings: &Settings,
        book: zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
        metadata: zero_platform_tokio::contract::component::PacketDriverMetadataView,
    ) -> std::io::Result<Self> {
        let copying = book.try_reserve(material::clone_bound(settings)?)?;
        let profile = Self::new_owned(settings.clone(), book, metadata)?;
        // Raw TCP settings were consumed; retained UDP backing already has
        // its distinct exact same-book custody before this clone peak returns.
        drop(copying);
        Ok(profile)
    }
    /// Pure identity for the actual immutable-material cache, never IO/close
    /// or Scope authority. A library profile cannot satisfy a Host binding.
    pub fn same_owned_budget(
        &self,
        book: &zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
        metadata: &zero_platform_tokio::contract::component::PacketDriverMetadataView,
    ) -> bool {
        self.tcp.same_owned_budget(book, metadata)
    }
}

impl Profile {
    /// The exact original SHA256 identity, without material allocation or IO.
    pub fn settings_identity(settings: &Settings) -> [u8; 32] {
        identity(settings)
    }
}
