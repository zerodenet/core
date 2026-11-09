//! Same wire custody before and after its actual registered input is handed in.
//! A missing logical path grants no IO and says nothing about native closure.
use super::*;
use std::sync::OnceLock;
use zero_platform_tokio::contract::component::PacketDriverMetadataView;
use zero_traits::packet_metadata::PacketMetadataArc;

#[derive(Clone)]
pub(in crate::runtime::raw_ip::device) enum Carrier {
    Existing(Arc<dyn RawIpWireCarrier>),
    Logical(PacketMetadataArc<Logical>),
}
pub(in crate::runtime::raw_ip::device) struct Logical {
    path: OnceLock<Arc<dyn RawIpWireCarrier>>,
    stopped: AtomicBool,
}
impl Carrier {
    pub(super) fn logical(memory: &PacketDriverMetadataView) -> Result<Self, EngineError> {
        let layout = PacketMetadataArc::<Logical>::layout().map_err(std::io::Error::other)?;
        let credit = memory.reserve(layout).map_err(std::io::Error::other)?;
        let original = Logical {
            path: OnceLock::new(),
            stopped: AtomicBool::new(false),
        };
        PacketMetadataArc::from_prepaid(original, credit)
            .map(Self::Logical)
            .map_err(|(source, original, credit)| {
                drop(original);
                drop(credit);
                std::io::Error::other(source).into()
            })
    }
    pub(super) fn install(&self, path: Arc<dyn RawIpWireCarrier>) -> Result<(), EngineError> {
        let Self::Logical(original) = self else {
            return Err(
                std::io::Error::other("wire input belongs to another carrier class").into(),
            );
        };
        if original.stopped.load(Ordering::Acquire) {
            // The caller keeps the same actual Path controller on failed handoff.
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "logical wire stopped before handoff",
            )
            .into());
        }
        original.path.set(path).map_err(|returned| {
            drop(returned);
            std::io::Error::other("same logical wire already received its actual Path")
        })?;
        // Stop can race between the precheck and strong installation. The same
        // controller remains retained and is fenced; no path escapes to IO.
        if original.stopped.load(Ordering::Acquire) {
            drop(
                original
                    .path
                    .get()
                    .expect("same installed Path")
                    .stop_background(),
            );
            return Err(super::super::super::carrier::background_stopped().into());
        }
        Ok(())
    }
    fn active(&self) -> std::io::Result<&Arc<dyn RawIpWireCarrier>> {
        match self {
            Self::Existing(path) => Ok(path),
            Self::Logical(original) if !original.stopped.load(Ordering::Acquire) => original
                .path
                .get()
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotConnected)),
            Self::Logical(_) => Err(super::super::super::carrier::background_stopped()),
        }
    }
    pub(in crate::runtime::raw_ip::device) async fn recv(
        &self,
        bytes: &mut [u8],
    ) -> std::io::Result<(usize, Option<SocketAddr>)> {
        self.active()?.recv(bytes).await
    }
    pub(in crate::runtime::raw_ip::device) async fn send(
        &self,
        bytes: &[u8],
        endpoint: SocketAddr,
    ) -> std::io::Result<()> {
        self.active()?.send(bytes, endpoint).await
    }
    pub(in crate::runtime::raw_ip::device) fn stop_background(
        &self,
    ) -> Option<super::super::super::carrier::CarrierWorkerCompletion> {
        match self {
            Self::Existing(path) => path.stop_background(),
            Self::Logical(original) => {
                original.stopped.store(true, Ordering::Release);
                match original.path.get() {
                    Some(path) => path.stop_background(),
                    None => Some(Box::pin(async { Ok(()) })),
                }
            }
        }
    }
    pub(in crate::runtime::raw_ip::device) async fn close_native(&self) -> Result<(), EngineError> {
        match self {
            Self::Existing(path) => path.close_native().await,
            Self::Logical(original) => match original.path.get() {
                Some(path) => path.close_native().await,
                None => Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "logical wire has no physical ancestor close receipt",
                )
                .into()),
            },
        }
    }
}
