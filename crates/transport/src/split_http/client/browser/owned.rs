//! One original Browser GET plus ordered POSTs, driven by its actual Factory.
//! Preparation opens no socket and starts no task. This is not an XMUX pool.
mod accounting;
mod driver;
mod network;
use super::*;
use crate::browser_dialer::PreparedBrowserLoan;
use crate::split_http::{
    io::LifetimeRef,
    xmux::owned::{OwnedLogicalReturn, RequestCauses, WorkReturn},
};
use zero_platform_tokio::{
    contract::{
        component::PacketDriverMetadataView,
        network::{CarrierContext, UpstreamTransport},
        ChannelError, Impact, Reason, Stage,
    },
    controlled_carrier::{
        loan::StreamLoan, CarrierBufferBudget, CarrierBufferCredit, CarrierBufferWorkingLane,
        CarrierIo,
    },
};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataCredit};
const DUPLEX_BYTES: usize = 64 * 1024;

pub struct PreparedBrowserXhttp {
    stream: XhttpStream,
    driver: BrowserXhttpDriver,
    get: PreparedBrowserLoan,
}
pub struct BrowserXhttpGetReady {
    loan: StreamLoan,
    stream: XhttpStream,
    driver: BrowserXhttpDriver,
}
pub struct BrowserXhttpDriver {
    network: tokio::io::DuplexStream,
    profile: Profile,
    scheme: String,
    authority: String,
    session: [u8; 32],
    dialer: BrowserDialer,
    life: LifetimeRef,
    causes: PacketMetadataArc<RequestCauses>,
    upload: accounting::Buffer,
    download: accounting::Buffer,
    transmit: CarrierBufferWorkingLane,
    control: CarrierBufferWorkingLane,
    _returned: OwnedLogicalReturn,
    _work: WorkReturn,
    _profile: PacketMetadataCredit,
    _authority: CarrierBufferCredit,
}
fn failure(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Session).with_source(source)
}
impl PreparedBrowserXhttp {
    pub fn prepare(
        dialer: BrowserDialer,
        config: &crate::profile::OwnedSplitHttpProfile,
        scheme: &str,
        authority: &str,
        book: &CarrierBufferBudget,
        memory: &PacketDriverMetadataView,
    ) -> Result<Self, ChannelError> {
        if !matches!(scheme, "http" | "https")
            || !matches!(
                XhttpMode::parse(&config.mode),
                XhttpMode::Auto | XhttpMode::PacketUp
            )
        {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                Impact::Session,
            ));
        }
        let get = PreparedBrowserLoan::prepare(dialer.clone(), book, memory)?;
        let (profile, profile_memory) = accounting::profile(config, memory).map_err(failure)?;
        let address_peak = authority
            .len()
            .checked_mul(8)
            .and_then(|n| n.checked_add(512))
            .ok_or_else(|| failure(io::Error::from(io::ErrorKind::InvalidInput)))?;
        let authority_memory = book.try_reserve(address_peak).map_err(failure)?;
        let authority = super::normalize_authority(authority).map_err(|source| {
            ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Session)
                .with_source(source)
        })?;
        let control_peak =
            driver::control_peak(&profile, scheme.len(), authority.len()).map_err(failure)?;
        let control = book.try_working_lane(control_peak).map_err(failure)?;
        let scratch = (profile.options.sc_max_each_post_bytes.to.max(1) as usize).min(DUPLEX_BYTES);
        let transmit = book
            .try_working_lane(profile.transmit_peak(scratch).map_err(failure)?)
            .map_err(failure)?;
        let upload = accounting::Buffer::prepare(book, scratch).map_err(failure)?;
        let download = accounting::Buffer::prepare(book, 16 * 1024).map_err(failure)?;
        let duplex = book.try_reserve(2 * DUPLEX_BYTES).map_err(failure)?;
        let life = LifetimeRef::prepare(memory).map_err(failure)?;
        let causes = RequestCauses::prepare(memory).map_err(failure)?;
        let returned =
            OwnedLogicalReturn::prepare(memory, life.clone(), duplex).map_err(failure)?;
        let work = returned.enter().map_err(failure)?;
        let (mut stream, network) = super::super::stream_pair_with_lifetime(life.clone());
        stream.owned_return = Some(returned.clone());
        let mut session = [0; 32];
        for (value, pair) in rand::random::<u128>()
            .to_be_bytes()
            .into_iter()
            .zip(session.chunks_mut(2))
        {
            pair[0] = b"0123456789abcdef"[(value >> 4) as usize];
            pair[1] = b"0123456789abcdef"[(value & 15) as usize];
        }
        Ok(Self {
            stream,
            driver: BrowserXhttpDriver {
                network,
                profile,
                scheme: scheme.to_owned(),
                authority,
                session,
                dialer,
                life,
                causes,
                upload,
                download,
                transmit,
                control,
                _returned: returned,
                _work: work,
                _profile: profile_memory,
                _authority: authority_memory,
            },
            get,
        })
    }
    /// Use the actual Entry's persistent cancellation fence. The first query's
    /// cancellation governs only initialization; it cannot own cached I/O.
    pub async fn open_get(
        self,
        upstream: &UpstreamTransport,
        call: &CarrierContext,
    ) -> Result<BrowserXhttpGetReady, ChannelError> {
        let Self {
            stream,
            driver,
            get,
        } = self;
        let _control = driver
            .control
            .reserve(driver.control.bound())
            .await
            .map_err(failure)?;
        let session = std::str::from_utf8(&driver.session).expect("fixed lowercase hex");
        let request = driver
            .profile
            .request("GET", session, None, Body::empty())
            .map_err(failure)?;
        let url = super::absolute_url(&driver.scheme, &driver.authority, request.uri())
            .map_err(failure)?;
        let loan = tokio::time::timeout(
            Duration::from_secs(30),
            get.get(&url, request.headers(), upstream, call),
        )
        .await
        .map_err(|source| {
            ChannelError::new(Stage::Handshake, Reason::Timeout, Impact::Session)
                .with_source(source)
        })??;
        Ok(BrowserXhttpGetReady {
            loan,
            stream,
            driver,
        })
    }
}
impl BrowserXhttpGetReady {
    /// The Factory retains/adopts this exact GET loan and publishes its actual
    /// driver witness before polling Driver::run with the matching CarrierIo.
    pub fn into_parts(self) -> (StreamLoan, XhttpStream, BrowserXhttpDriver) {
        (self.loan, self.stream, self.driver)
    }
}
