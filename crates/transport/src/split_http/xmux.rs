//! HTTP client-group pooling. Logical streams own usage leases, while HTTP
//! drivers and response bodies retain connections independently of those leases.
mod allocations;
use allocations::{GroupRef, Groups, UsageRef};
mod connection;
mod group;
pub(in crate::split_http) mod group_body;
mod owner;
use owner::OwnerKey;
pub(crate) mod owned;
mod owned_connect;
pub use owned_connect::OwnedXhttpContext;
pub(super) mod prepared;
pub(super) mod request;
use super::{client, request::Profile, XhttpMode, XhttpStream};
use group::Group;
pub(super) use group::Usage;
use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
};
use zero_traits::{SplitHttpRange, SplitHttpTransportProfile, SplitHttpXmux};

pub enum XhttpCarrier {
    Http1(zero_platform_tokio::TcpRelayStream),
    Http2(zero_platform_tokio::TcpRelayStream),
    Http3(crate::quic::QuicConnection),
    OwnedHttp3(owned::OwnedXhttp3),
}
pub type XhttpCarrierFactory = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<XhttpCarrier, crate::RuntimeError>> + Send>>
        + Send
        + Sync,
>;
struct State {
    groups: Groups,
    concurrency: u32,
    connections: u32,
}
struct Inner {
    config: SplitHttpXmux,
    state: Mutex<State>,
    maintenance: std::sync::atomic::AtomicBool,
}
#[derive(Clone)]
pub struct XhttpClientPool(Arc<Inner>);
impl fmt::Debug for XhttpClientPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("XhttpClientPool").finish_non_exhaustive()
    }
}
impl XhttpClientPool {
    pub fn new(mut config: SplitHttpXmux) -> Self {
        if config == SplitHttpXmux::default() {
            config.max_concurrency = SplitHttpRange::new(1, 1);
            config.h_max_request_times = SplitHttpRange::new(600, 900);
            config.h_max_reusable_secs = SplitHttpRange::new(1800, 3000);
        }
        Self(Arc::new(Inner {
            config,
            maintenance: std::sync::atomic::AtomicBool::new(false),
            state: Mutex::new(State {
                groups: Groups::default(),
                concurrency: super::request::sample(config.max_concurrency),
                connections: super::request::sample(config.max_connections),
            }),
        }))
    }
    /// Retire groups on reload without killing active response bodies.
    pub fn retire(&self) {
        let retired = {
            let mut state = self.0.state.lock().unwrap();
            state.groups.take()
        };
        drop(retired);
    }
    fn reap(&self) {
        Self::reap_inner(&self.0);
    }
    fn reap_inner(pool: &Inner) {
        loop {
            let retired = {
                let mut state = pool.state.lock().unwrap();
                state
                    .groups
                    .iter()
                    .position(|group| !group.reusable())
                    .map(|index| state.groups.remove(index))
            };
            match retired {
                Some(group) => drop(group),
                None => break,
            }
        }
    }
    fn select(&self, owner: Option<&OwnerKey>) -> io::Result<Usage> {
        // Reserve/refund notifications are held before the pool gate. Real
        // Group/connection destructors remain in reap/retire's lock-free exit.
        let _notifications = owner.map(|owner| owner.memory.notifications());
        self.reap();
        let mut state = self.0.state.lock().unwrap();
        if let Some(owner) = owner {
            state.groups.bind(&owner.memory)?;
        }
        let mut available = 0usize;
        let mut chosen = None;
        let mut owner_groups = 0;
        for group in state.groups.iter() {
            if !group.matches_owner(owner) {
                continue;
            }
            owner_groups += 1;
            if state.concurrency == 0 || group.active() < state.concurrency as usize {
                available += 1;
                if rand::random_range(0..available) == 0 {
                    chosen = Some(group.clone());
                }
            }
        }
        let create = owner_groups == 0
            || available == 0
            || (state.connections > 0 && owner_groups < state.connections as usize);
        let group = if create {
            let group = GroupRef::new(self.0.config, owner.cloned())?;
            state.groups.push(group.clone())?;
            group
        } else {
            chosen.expect("same actual available group")
        };
        Ok(Usage::new(group))
    }
    pub async fn connect<P: SplitHttpTransportProfile + ?Sized>(
        &self,
        factory: XhttpCarrierFactory,
        config: &P,
    ) -> Result<XhttpStream, crate::RuntimeError> {
        self.connect_with_download(factory, config, None).await
    }
    pub async fn connect_with_download<P: SplitHttpTransportProfile + ?Sized>(
        &self,
        factory: XhttpCarrierFactory,
        config: &P,
        download: Option<XhttpDownload<'_>>,
    ) -> Result<XhttpStream, crate::RuntimeError> {
        if !self
            .0
            .maintenance
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            let weak = Arc::downgrade(&self.0);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(15)).await;
                    let Some(pool) = weak.upgrade() else {
                        break;
                    };
                    XhttpClientPool::reap_inner(&pool);
                }
            });
        }
        let profile = Profile::new(config);
        let usage = UsageRef::new(self.select(None)?, None)?;
        let (mut stream, network) = client::stream_pair();
        stream.usages.push(usage.clone());
        let sender = request::Sender {
            pool: self.clone(),
            factory,
            usage,
            owned: false,
            owner: None,
            logical: None,
        };
        if profile.mode == XhttpMode::StreamOne {
            if download.is_some() {
                return Err(io::Error::other("stream-one cannot split download").into());
            }
            return super::http3::client::single(
                client::carrier::Sender::Xmux(sender),
                stream,
                network,
                profile,
            );
        }
        let (download, download_profile) = if let Some(download) = download {
            let usage = UsageRef::new(download.pool.select(None)?, None)?;
            stream.usages.push(usage.clone());
            (
                request::Sender {
                    pool: download.pool.clone(),
                    factory: download.factory,
                    usage,
                    owned: false,
                    owner: None,
                    logical: None,
                },
                Profile::new(download.config),
            )
        } else {
            (sender.clone(), profile.clone())
        };
        client::connect_profiles(
            client::carrier::Sender::Xmux(sender),
            client::carrier::Sender::Xmux(download),
            stream,
            network,
            profile,
            download_profile,
        )
        .await
    }
}
pub struct XhttpDownload<'a> {
    pub pool: &'a XhttpClientPool,
    pub factory: XhttpCarrierFactory,
    pub config: &'a (dyn SplitHttpTransportProfile + Sync),
}
