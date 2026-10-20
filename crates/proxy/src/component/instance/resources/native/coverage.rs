//! Bounded constructor bindings into the canonical declaration list. These
//! contain no recipes, process arguments/environment or historical resources.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::component) enum DeclaredResourceKind {
    ControlledOutboundStream,
    ControlledDirectStream,
    NativeIcmp,
    MetadataDatagram,
    File,
    TcpListener,
    DatagramListener,
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
            ResourceDeclaration::ControlledOutboundStream { .. } => {
                Self::Carrier(DeclaredResourceKind::ControlledOutboundStream)
            }
            ResourceDeclaration::ControlledDirectStream { .. } => {
                Self::Carrier(DeclaredResourceKind::ControlledDirectStream)
            }
            ResourceDeclaration::MetadataDatagram { .. } => Self::Metadata,
            ResourceDeclaration::NativeIcmp { .. } => {
                Self::Unaccounted(DeclaredResourceKind::NativeIcmp)
            }
            ResourceDeclaration::File { path, .. } => Self::File(path.clone()),
            ResourceDeclaration::TcpListener { destination, .. } => {
                Self::Tcp(sockets::literal(component, destination)?)
            }
            ResourceDeclaration::DatagramListener { destination, .. } => {
                Self::Datagram(sockets::literal(component, destination)?)
            }
            ResourceDeclaration::Process { .. } => Self::Unaccounted(DeclaredResourceKind::Process),
            ResourceDeclaration::Device { .. } => Self::Unaccounted(DeclaredResourceKind::Device),
            ResourceDeclaration::BackgroundCarrier => {
                Self::Unaccounted(DeclaredResourceKind::BackgroundCarrier)
            }
            ResourceDeclaration::OutboundDatagram { .. } => {
                Self::Unaccounted(DeclaredResourceKind::OutboundDatagram)
            }
            ResourceDeclaration::BoundOutboundDatagram { .. } => {
                Self::Unaccounted(DeclaredResourceKind::BoundOutboundDatagram)
            }
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
        let mut native = vec![false; self.native.len()];
        let mut carriers = vec![false; self.carriers.len()];
        let mut metadata = vec![false; self.metadata.len()];
        for (ordinal, declaration) in self.declarations.iter().enumerate() {
            if declaration.ordinal != ordinal {
                return false;
            }
            let confirmed = match declaration.provider {
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
                _ => false,
            };
            if !confirmed {
                return false;
            }
        }
        files.into_iter().all(|seen| seen)
            && native.into_iter().all(|seen| seen)
            && carriers.into_iter().all(|seen| seen)
            && metadata.into_iter().all(|seen| seen)
    }
}

#[cfg(test)]
#[path = "../../../tests/resource_coverage.rs"]
mod tests;
