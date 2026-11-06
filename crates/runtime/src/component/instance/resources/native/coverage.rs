//! Bounded constructor bindings into the canonical declaration list. These
//! contain no recipes, process arguments/environment or historical resources.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::component) enum DeclaredResourceKind {
    ControlledOutboundStream,
    ControlledCarrierPool,
    ControlledDatagramCarrier,
    ControlledOutboundDatagram,
    ControlledDirectStream,
    ControlledPacketDevice,
    ControlledNodeStream,
    NativeIcmp,
    MetadataDatagram,
    ControlledMetadataDatagram,
    File,
    FileTree,
    TcpListener,
    DatagramListener,
    AcceptedDatagram,
    DatagramAssociations,
    InboundTargets,
    Process,
    Device,
    BackgroundCarrier,
    OutboundDatagram,
    BoundOutboundDatagram,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::component) enum DeclarationProvider {
    File(usize),
    Native(usize),
    Carrier(usize),
    Metadata(usize),
    MetadataPool(usize),
    RootDatagram(usize),
    PacketDevice(usize),
    BoundDatagram(usize),
    Unaccounted,
}
#[derive(Clone, Debug)]
pub(in crate::component) struct DeclarationEvidence {
    pub ordinal: usize,
    pub kind: DeclaredResourceKind,
    /// Index into this same snapshot's actual File/native observations. The
    /// index supplies coverage only; its actual witness must still confirm.
    pub provider: DeclarationProvider,
}
pub(super) enum CoverageBinding {
    Carrier(DeclaredResourceKind),
    Metadata,
    MetadataPool,
    RootDatagram,
    PacketDevice,
    BoundDatagram,
    File(PathBuf),
    Tcp(SocketAddr),
    Datagram(SocketAddr),
    Unaccounted(DeclaredResourceKind),
}
impl CoverageBinding {
    pub(super) fn new(
        component: ComponentId,
        declaration: &ResourceDeclaration,
    ) -> Result<Self, ChannelError> {
        Ok(match declaration {
            ResourceDeclaration::ControlledPacketDevice { .. } => Self::PacketDevice,
            ResourceDeclaration::ControlledOutboundDatagram { .. } => Self::RootDatagram,
            ResourceDeclaration::ControlledCarrierPool { .. }
            | ResourceDeclaration::ControlledAssociationPool { .. } => {
                Self::Carrier(DeclaredResourceKind::ControlledCarrierPool)
            }
            ResourceDeclaration::ControlledOutboundStream { .. } => {
                Self::Carrier(DeclaredResourceKind::ControlledOutboundStream)
            }
            ResourceDeclaration::ControlledDatagramCarrier { .. } => {
                Self::Carrier(DeclaredResourceKind::ControlledDatagramCarrier)
            }
            ResourceDeclaration::ControlledNodeStream { .. } => {
                Self::Carrier(DeclaredResourceKind::ControlledNodeStream)
            }
            ResourceDeclaration::ControlledDirectStream { .. } => {
                Self::Carrier(DeclaredResourceKind::ControlledDirectStream)
            }
            ResourceDeclaration::ListenerHandoff { .. } => {
                return Err(super::error(
                    component,
                    zero_platform_tokio::contract::Reason::Unsupported,
                    "listener handoff requires its actual inbound host originals",
                ))
            }
            ResourceDeclaration::MetadataDatagram { .. } => Self::Metadata,
            ResourceDeclaration::ControlledMetadataDatagram { .. }
            | ResourceDeclaration::ControlledMetadataCarrier { .. } => Self::MetadataPool,
            ResourceDeclaration::NativeIcmp { .. } => {
                Self::Unaccounted(DeclaredResourceKind::NativeIcmp)
            }
            ResourceDeclaration::FileTree { .. } => {
                Self::Unaccounted(DeclaredResourceKind::FileTree)
            }
            ResourceDeclaration::File { path, .. } => Self::File(path.clone()),
            ResourceDeclaration::TcpListener { destination, .. } => {
                Self::Tcp(sockets::literal(component, destination)?)
            }
            ResourceDeclaration::DatagramListener { destination, .. } => {
                Self::Datagram(sockets::literal(component, destination)?)
            }
            // This requires the actual accepted entry's original provider.
            // A normal catalog declaration alone must never qualify it.
            ResourceDeclaration::InboundTargets { .. } => {
                Self::Unaccounted(DeclaredResourceKind::InboundTargets)
            }
            ResourceDeclaration::DatagramAssociations { .. } => {
                Self::Unaccounted(DeclaredResourceKind::DatagramAssociations)
            }
            ResourceDeclaration::AcceptedDatagram { .. } => {
                Self::Unaccounted(DeclaredResourceKind::AcceptedDatagram)
            }
            ResourceDeclaration::Process { .. } => Self::Unaccounted(DeclaredResourceKind::Process),
            ResourceDeclaration::Device { .. } => Self::Unaccounted(DeclaredResourceKind::Device),
            ResourceDeclaration::BackgroundCarrier => {
                Self::Unaccounted(DeclaredResourceKind::BackgroundCarrier)
            }
            ResourceDeclaration::OutboundDatagram { .. } => {
                Self::Unaccounted(DeclaredResourceKind::OutboundDatagram)
            }
            ResourceDeclaration::BoundOutboundDatagram { .. } => Self::BoundDatagram,
        })
    }
    pub(super) fn observe(
        &self,
        ordinal: usize,
        files: &[evidence::FileIoEvidence],
        native: &[evidence::NativeIoEvidence],
    ) -> DeclarationEvidence {
        let (kind, provider) = match self {
            Self::Carrier(kind) => (*kind, None),
            Self::Metadata => (DeclaredResourceKind::MetadataDatagram, None),
            Self::MetadataPool => (DeclaredResourceKind::ControlledMetadataDatagram, None),
            Self::RootDatagram => (DeclaredResourceKind::ControlledOutboundDatagram, None),
            Self::PacketDevice => (DeclaredResourceKind::ControlledPacketDevice, None),
            Self::BoundDatagram => (DeclaredResourceKind::BoundOutboundDatagram, None),
            Self::File(path) => (
                DeclaredResourceKind::File,
                files
                    .iter()
                    .position(|file| &file.path == path)
                    .map(DeclarationProvider::File),
            ),
            Self::Tcp(address) => (
                DeclaredResourceKind::TcpListener,
                native
                    .iter()
                    .position(|owner| owner.tcp && &owner.address == address)
                    .map(DeclarationProvider::Native),
            ),
            Self::Datagram(address) => (
                DeclaredResourceKind::DatagramListener,
                native
                    .iter()
                    .position(|owner| !owner.tcp && &owner.address == address)
                    .map(DeclarationProvider::Native),
            ),
            Self::Unaccounted(kind) => (*kind, None),
        };
        DeclarationEvidence {
            ordinal,
            kind,
            provider: provider.unwrap_or(DeclarationProvider::Unaccounted),
        }
    }
}
impl evidence::HostIoEvidence {
    /// Physical facts only. Slot must also close admission, complete protocol
    /// cleanup and join its actual scope/transferred child graph before sealing.
    pub(in crate::component) fn physical_confirmed(&self) -> bool {
        if self.declarations.len() != self.declared_count {
            return false;
        }
        let mut files = vec![false; self.files.len()];
        let mut metapools = vec![false; self.metapools.len()];
        let mut native = vec![false; self.native.len()];
        let mut carriers = vec![false; self.carriers.len()];
        let mut metadata = vec![false; self.metadata.len()];
        let mut roots = vec![false; self.roots.len()];
        let mut packets = vec![false; self.packets.len()];
        let mut bounds = vec![false; self.bounds.len()];
        for (ordinal, declaration) in self.declarations.iter().enumerate() {
            if declaration.ordinal != ordinal {
                return false;
            }
            let confirmed = match declaration.provider {
                DeclarationProvider::BoundDatagram(index)
                    if declaration.kind == DeclaredResourceKind::BoundOutboundDatagram =>
                {
                    match (self.bounds.get(index), bounds.get_mut(index)) {
                        (Some(owner), Some(seen)) if !*seen && owner.ordinal == ordinal => {
                            *seen = true;
                            owner.physical_confirmed()
                        }
                        _ => false,
                    }
                }

                DeclarationProvider::PacketDevice(index)
                    if declaration.kind == DeclaredResourceKind::ControlledPacketDevice =>
                {
                    match (self.packets.get(index), packets.get_mut(index)) {
                        (Some(owner), Some(seen)) if !*seen && owner.ordinal == ordinal => {
                            *seen = true;
                            owner.physical_confirmed()
                        }
                        _ => false,
                    }
                }

                DeclarationProvider::File(index)
                    if declaration.kind == DeclaredResourceKind::File =>
                {
                    match (self.files.get(index), files.get_mut(index)) {
                        (Some(file), Some(seen)) if !*seen => {
                            *seen = true;
                            file.physical_confirmed()
                        }
                        _ => false,
                    }
                }
                DeclarationProvider::Native(index) => {
                    match (self.native.get(index), native.get_mut(index)) {
                        (Some(owner), Some(seen))
                            if !*seen
                                && matches!(
                                    (declaration.kind, owner.tcp),
                                    (DeclaredResourceKind::TcpListener, true)
                                        | (DeclaredResourceKind::DatagramListener, false)
                                ) =>
                        {
                            *seen = true;
                            owner.confirmed || owner.known_no_acquisition
                        }
                        _ => false,
                    }
                }
                DeclarationProvider::Metadata(index)
                    if declaration.kind == DeclaredResourceKind::MetadataDatagram =>
                {
                    match (self.metadata.get(index), metadata.get_mut(index)) {
                        (Some(owner), Some(seen)) if !*seen && owner.ordinal == ordinal => {
                            *seen = true;
                            owner.physical_confirmed()
                        }
                        _ => false,
                    }
                }
                DeclarationProvider::Carrier(index)
                    if matches!(
                        declaration.kind,
                        DeclaredResourceKind::ControlledOutboundStream
                            | DeclaredResourceKind::ControlledCarrierPool
                            | DeclaredResourceKind::ControlledDatagramCarrier
                            | DeclaredResourceKind::ControlledNodeStream
                            | DeclaredResourceKind::ControlledDirectStream
                    ) =>
                {
                    match (self.carriers.get(index), carriers.get_mut(index)) {
                        (Some(owner), Some(seen))
                            if !*seen
                                && owner.ordinal == ordinal
                                && owner.kind == declaration.kind =>
                        {
                            *seen = true;
                            owner.physical_confirmed()
                        }
                        _ => false,
                    }
                }
                DeclarationProvider::RootDatagram(index)
                    if declaration.kind == DeclaredResourceKind::ControlledOutboundDatagram =>
                {
                    match (self.roots.get(index), roots.get_mut(index)) {
                        (Some(owner), Some(seen)) if !*seen && owner.ordinal == ordinal => {
                            *seen = true;
                            owner.physical_confirmed()
                        }
                        _ => false,
                    }
                }
                DeclarationProvider::MetadataPool(index)
                    if declaration.kind == DeclaredResourceKind::ControlledMetadataDatagram =>
                {
                    match (self.metapools.get(index), metapools.get_mut(index)) {
                        (Some(owner), Some(seen)) if !*seen && owner.ordinal == ordinal => {
                            *seen = true;
                            owner.physical_confirmed()
                        }
                        _ => false,
                    }
                }
                _ => false,
            };
            if !confirmed {
                return false;
            }
        }
        files.into_iter().all(|seen| seen)
            && metapools.into_iter().all(|seen| seen)
            && native.into_iter().all(|seen| seen)
            && carriers.into_iter().all(|seen| seen)
            && metadata.into_iter().all(|seen| seen)
            && roots.into_iter().all(|seen| seen)
            && packets.into_iter().all(|seen| seen)
            && bounds.into_iter().all(|seen| seen)
    }
}

#[cfg(test)]
#[path = "../../../tests/resource_coverage.rs"]
mod tests;
