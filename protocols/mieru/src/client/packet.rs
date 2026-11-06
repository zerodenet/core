//! One packet driver preparation, shared by legacy and supervised execution.
use super::*;
#[cfg(feature = "runtime")]
mod native;
#[cfg(feature = "runtime")]
pub(super) use native::NativePacketPreparation;

pub(super) struct Prepared {
    pub(super) first: Option<crate::inbound::multiplex::stream::MieruLogicalStream>,
    pub(super) opens: mpsc::Sender<OpenRequest>,
    pub(super) driver: std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>,
}
pub(super) fn prepare(
    carrier: Arc<dyn ClientDatagramCarrier>,
    ipv6: bool,
    username: &str,
    password: &str,
    options: &mieru_config::MieruTransportOptions,
) -> io::Result<Prepared> {
    prepare_inner(carrier, ipv6, username, password, options, false)
}
#[cfg(feature = "runtime")]
pub(super) fn prepare_native(
    carrier: Arc<dyn ClientDatagramCarrier>,
    ipv6: bool,
    username: &str,
    password: &str,
    options: &mieru_config::MieruTransportOptions,
) -> io::Result<Prepared> {
    prepare_inner(carrier, ipv6, username, password, options, true)
}
fn prepare_inner(
    carrier: Arc<dyn ClientDatagramCarrier>,
    ipv6: bool,
    username: &str,
    password: &str,
    options: &mieru_config::MieruTransportOptions,
    first: bool,
) -> io::Result<Prepared> {
    options.validate().map_err(io::Error::other)?;
    use crate::{
        inbound::multiplex::reader::Reader,
        packet::{Driver, PacketCodec, PacketIo},
    };
    let (ready, _unused) = mpsc::channel(1);
    let (outgoing, commands) = mpsc::channel(64);
    let (dropped, drop_events) = mpsc::unbounded_channel();
    let (opens, requests) = mpsc::channel(64);
    let key = crate::crypto::derive_key(
        username,
        password,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_secs(),
    );
    let book = Reader::new(ready, outgoing, dropped, options.receive.clone());
    let mut driver = Driver::new(
        book,
        PacketCodec::configured(key, username, options, ipv6)?,
        PacketIo::Client { carrier },
        true,
    );
    let first = first.then(|| driver.open_client()).transpose()?;
    Ok(Prepared {
        first,
        opens,
        driver: Box::pin(crate::packet::run(driver, commands, requests, drop_events)),
    })
}
impl ClientConnection {
    pub(super) fn from_packet(opens: mpsc::Sender<OpenRequest>, task: DriverHandle) -> Arc<Self> {
        Arc::new(Self {
            first: Mutex::new(None),
            opens,
            task,
            active_streams: AtomicUsize::new(0),
            pending_opens: AtomicUsize::new(0),
            max_streams: 64,
            created_at: Instant::now(),
        })
    }
}
