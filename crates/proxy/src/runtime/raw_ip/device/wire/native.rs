//! One unique native input to the already existing wire driver, not a task ACK.
use super::*;
use zero_platform_tokio::contract::{component::GrantedBoundDatagram, scope::TaskJoinWitness};
use zero_tun::packet_memory::NativeReadGrant;
pub(crate) struct PendingNativeWire {
    input: tokio::sync::oneshot::Sender<NativeWireGrant>,
    pending: tokio::sync::oneshot::Receiver<()>,
}
pub(in crate::runtime::raw_ip::device) struct NativeWireInput {
    receiver: tokio::sync::oneshot::Receiver<NativeWireGrant>,
    pending: Option<tokio::sync::oneshot::Sender<()>>,
    pub(super) carrier: Carrier,
    hosted: Option<Arc<super::super::super::carrier::HostedRawIpWireCarrier>>,
    pub(super) workers: std::sync::Weak<super::super::workers::DeviceWorkers>,
}
struct NativeWireGrant {
    original: WireInputOriginal,
    reads: WireReads,
    driver: TaskJoinWitness,
    checkpoint: crate::runtime::packet_route::host::pause::Checkpoint,
}
enum WireInputOriginal {
    Bound(zero_platform_tokio::contract::component::BoundDatagramSocket),
    Logical(Arc<super::super::super::carrier::RegisteredProxiedRawIpWireCarrier>),
}
pub(in crate::runtime::raw_ip::device) struct WireReads {
    pub reads: [NativeReadGrant; 2],
    pub control: NativeReadGrant,
}
impl WireReads {
    pub(in crate::runtime::raw_ip::device) fn memory(
        &self,
    ) -> zero_tun::packet_memory::PacketDatagramWorkspaces {
        zero_tun::packet_memory::PacketDatagramWorkspaces {
            reads: self.reads.clone(),
            control: Some(self.control.clone()),
        }
    }
}
impl Drop for WireReads {
    fn drop(&mut self) {
        for grant in self.reads.iter().chain(std::iter::once(&self.control)) {
            grant.revoke();
        }
    }
}
impl NativeWireInput {
    pub(super) fn channel(
        carrier: Arc<super::super::super::carrier::HostedRawIpWireCarrier>,
    ) -> (Self, PendingNativeWire) {
        Self::make_channel(Carrier::Existing(carrier.clone()), Some(carrier))
    }
    pub(super) fn logical_channel(carrier: Carrier) -> (Self, PendingNativeWire) {
        Self::make_channel(carrier, None)
    }
    fn make_channel(
        carrier: Carrier,
        hosted: Option<Arc<super::super::super::carrier::HostedRawIpWireCarrier>>,
    ) -> (Self, PendingNativeWire) {
        let (send, receiver) = tokio::sync::oneshot::channel();
        let (pending, take_pending) = tokio::sync::oneshot::channel();
        (
            Self {
                receiver,
                pending: Some(pending),
                carrier,
                hosted,
                workers: std::sync::Weak::new(),
            },
            PendingNativeWire {
                input: send,
                pending: take_pending,
            },
        )
    }
    pub(in crate::runtime::raw_ip::device) async fn receive(
        mut self,
    ) -> Result<
        Option<(
            WireReads,
            crate::runtime::packet_route::host::pause::Checkpoint,
        )>,
        EngineError,
    > {
        let grant = std::future::poll_fn(|cx| {
            use std::future::Future;
            let result = std::pin::Pin::new(&mut self.receiver).poll(cx);
            if result.is_pending() {
                if let Some(pending) = self.pending.take() {
                    let _ = pending.send(());
                }
            }
            result
        })
        .await;
        let Ok(grant) = grant else {
            // The real wire never received a unique native view. Acquisition
            // errors/physical outcomes remain with lifecycle and HostResources;
            // channel completion is neither KnownNoAcquisition nor close proof.
            return Ok(None);
        };
        let actual = self
            .workers
            .upgrade()
            .and_then(|workers| workers.driver_witness())
            .ok_or_else(|| {
                std::io::Error::other("same wire registration was released before native input")
            })?;
        if !actual.same_registration(&grant.driver) || !grant.checkpoint.matches_driver(&actual) {
            return Err(
                std::io::Error::other("native input names another driver registration").into(),
            );
        }
        let reads = grant.reads;
        match grant.original {
            WireInputOriginal::Bound(socket) => {
                self.hosted
                    .as_ref()
                    .ok_or_else(|| std::io::Error::other("bound input belongs to a logical wire"))?
                    .install(socket, &actual)
                    .await?
            }
            WireInputOriginal::Logical(path) => {
                if self.hosted.is_some() || !path.matches_driver(&actual) {
                    return Err(
                        std::io::Error::other("logical input names another actual wire").into(),
                    );
                }
                self.carrier.install(path)?;
            }
        }
        Ok(Some((reads, grant.checkpoint)))
    }
}
impl PendingNativeWire {
    pub(crate) async fn wait_input_pending(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), EngineError> {
        tokio::time::timeout_at(deadline, &mut self.pending)
            .await
            .map_err(std::io::Error::other)?
            .map_err(|source| std::io::Error::other(source).into())
    }
    pub(crate) fn install_logical(
        self,
        path: Arc<super::super::super::carrier::RegisteredProxiedRawIpWireCarrier>,
        reads: [NativeReadGrant; 2],
        control: NativeReadGrant,
        driver: TaskJoinWitness,
        checkpoint: crate::runtime::packet_route::host::pause::Checkpoint,
    ) -> Result<(), EngineError> {
        if !path.matches_driver(&driver) || !checkpoint.matches_driver(&driver) {
            return Err(
                std::io::Error::other("logical path/input names another actual wire").into(),
            );
        }
        let original = NativeWireGrant {
            original: WireInputOriginal::Logical(path),
            reads: WireReads { reads, control },
            driver,
            checkpoint,
        };
        self.input.send(original).map_err(|returned| {
            drop(returned);
            std::io::Error::other("same logical wire stopped before actual handoff")
        })?;
        Ok(())
    }
    pub(crate) fn install(
        self,
        grant: GrantedBoundDatagram,
        reads: [NativeReadGrant; 2],
        control: NativeReadGrant,
        driver: TaskJoinWitness,
        checkpoint: crate::runtime::packet_route::host::pause::Checkpoint,
    ) -> Result<(), EngineError> {
        let reads = WireReads { reads, control };
        let origin = grant
            .socket
            .original()
            .ok_or_else(|| std::io::Error::other("same bound wire has no original registration"))?;
        if !origin.driver().same_registration(&driver) || !checkpoint.matches_driver(&driver) {
            return Err(
                std::io::Error::other("same bound original has a foreign wire witness").into(),
            );
        }
        let original = NativeWireGrant {
            original: WireInputOriginal::Bound(grant.socket),
            reads,
            driver,
            checkpoint,
        };
        // Failed transfer drops only the unique view and revokes its same paid
        // read slots; the actual native controller remains with HostResources.
        self.input.send(original).map_err(|returned| {
            drop(returned);
            std::io::Error::other("original wire input closed before native transfer")
        })?;
        Ok(())
    }
}
