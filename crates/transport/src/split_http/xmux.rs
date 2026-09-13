//! HTTP client-group pooling. Logical streams own usage leases, while HTTP
//! drivers and response bodies retain connections independently of those leases.
mod connection;
mod group;
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
}
pub type XhttpCarrierFactory = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<XhttpCarrier, crate::RuntimeError>> + Send>>
        + Send
        + Sync,
>;
struct State {
    groups: Vec<Arc<Group>>,
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
                groups: Vec::new(),
                concurrency: super::request::sample(config.max_concurrency),
                connections: super::request::sample(config.max_connections),
            }),
        }))
    }
    /// Retire groups on reload without killing active response bodies.
    pub fn retire(&self) {
        self.0.state.lock().unwrap().groups.clear();
    }
    fn select(&self, factory: XhttpCarrierFactory) -> Usage {
        let mut state = self.0.state.lock().unwrap();
        state.groups.retain(|group| group.reusable());
        let available: Vec<_> = state
            .groups
            .iter()
            .filter(|group| state.concurrency == 0 || group.active() < state.concurrency as usize)
            .cloned()
            .collect();
        let create = state.groups.is_empty()
            || available.is_empty()
            || (state.connections > 0 && state.groups.len() < state.connections as usize);
        let group = if create {
            let group = Arc::new(Group::new(self.0.config, factory));
            state.groups.push(group.clone());
            group
        } else {
            available[rand::random_range(0..available.len())].clone()
        };
        Usage::new(group)
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
                    pool.state
                        .lock()
                        .unwrap()
                        .groups
                        .retain(|group| group.reusable());
                }
            });
        }
        let profile = Profile::new(config);
        let usage = Arc::new(Mutex::new(self.select(factory.clone())));
        let (mut stream, network) = client::stream_pair();
        stream.usages.push(usage.clone());
        let sender = request::Sender {
            pool: self.clone(),
            factory,
            usage,
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
            let usage = Arc::new(Mutex::new(download.pool.select(download.factory.clone())));
            stream.usages.push(usage.clone());
            (
                request::Sender {
                    pool: download.pool.clone(),
                    factory: download.factory,
                    usage,
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
