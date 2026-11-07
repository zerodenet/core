//! Concrete Data preparation shares this PeerTunnel's real SDK Session. This
//! metadata grants no host I/O, task registration or native close authority.
use super::*;
use zero_tun::packet_memory::NativeReadBuffer;

pub enum PreparedReceive<T> {
    Complete(T),
    Data(PreparedData),
}
pub struct PreparedData {
    identity: Arc<()>,
    packet: PacketBuffer,
    pub peer: Option<usize>,
}
impl PreparedData {
    pub fn into_parts(self) -> (Arc<()>, PacketBuffer, Option<usize>) {
        (self.identity, self.packet, self.peer)
    }
}
pub(super) struct PendingData {
    identity: std::sync::Weak<()>,
    commit: gotatun::noise::PreparedDataCommit,
}
impl PendingData {
    pub(super) fn live(&self) -> bool {
        self.identity.strong_count() != 0
    }
}

impl PeerTunnel {
    /// Non-Data keeps its original handshake/COOKIE path. Data uses the actual
    /// prepaid decode scratch and defers SDK replay/session/timer commitment.
    pub fn prepare_datagram_owned(
        &mut self,
        source: Option<SocketAddr>,
        original: PacketBuffer,
        decode: NativeReadBuffer,
        control: PacketBuffer,
    ) -> OwnedTunnelResult<PreparedReceive<ReceivedDatagram>> {
        if !owned::is_data_datagram(&original) {
            let output = self.receive_datagram_owned(source, original, decode, control);
            return OwnedTunnelResult {
                result: output.result.map(PreparedReceive::Complete),
                unused_control: output.unused_control,
            };
        }
        let mut result = self
            .prepare_data(&original, decode)
            .map(|packet| match packet {
                PreparedReceive::Data(data) => PreparedReceive::Data(data),
                PreparedReceive::Complete(()) => PreparedReceive::Complete(ReceivedDatagram {
                    actions: Vec::new(),
                    authenticated: true,
                }),
            });
        if let Err(failure) = &mut result {
            if failure.original.is_none() {
                failure.original = Some(original);
            }
        }
        // Decryption has finished reading ciphertext. Its same original read
        // allocation can now return; no packet/async consumer borrows it. The
        // decrypted allocation remains charged through the returned packet.
        OwnedTunnelResult {
            result,
            unused_control: Some(control),
        }
    }
    pub(super) fn prepare_data(
        &mut self,
        original: &PacketBuffer,
        mut decode: NativeReadBuffer,
    ) -> Result<PreparedReceive<()>, OwnedTunnelFailure> {
        let failure = |error, storage| OwnedTunnelFailure {
            error,
            storage,
            original: None,
        };
        check_wire_datagram(original).map_err(|error| failure(error, None))?;
        if self.prepared_data.as_ref().is_some_and(PendingData::live) {
            return Err(failure(
                TunnelError::Engine,
                Some(zero_traits::PacketDeriveError::Busy),
            ));
        }
        self.prepared_data = None; // abandoned pure token, never receive success
        if original.len() > decode.as_bytes().len() {
            return Err(failure(
                TunnelError::Engine,
                Some(zero_traits::PacketDeriveError::LayoutMismatch),
            ));
        }
        decode.as_mut_bytes()[..original.len()].copy_from_slice(original);
        let buffer = decode
            .into_packet(original.len())
            .unwrap_or_else(|_| unreachable!("same checked original length"));
        let scratch = Packet::from_owned_buffer(buffer).map_err(|original| OwnedTunnelFailure {
            error: TunnelError::Engine,
            storage: Some(zero_traits::PacketDeriveError::Unreserved),
            original: Some(original),
        })?;
        let WgKind::Data(data) = scratch
            .try_into_wg()
            .map_err(|_| failure(TunnelError::InvalidWirePacket, None))?
        else {
            return Err(failure(TunnelError::InvalidWirePacket, None));
        };
        let (commit, plain) = self
            .engine
            .prepare_owned_data(data)
            .map_err(|_| failure(TunnelError::Engine, None))?;
        if plain.is_empty() {
            self.engine
                .commit_owned_data(commit)
                .map_err(|_| failure(TunnelError::Engine, None))?;
            return Ok(PreparedReceive::Complete(()));
        }
        // These are the same checked IP decoder/truncation steps as the old
        // collect_result. Invalid authenticated IP still consumes replay just
        // as the original protocol did, without any application delivery.
        let plain = match plain.try_into_ipvx() {
            Ok(ip) => ip.either(
                |ip| owned_packet(ip.into_bytes()),
                |ip| owned_packet(ip.into_bytes()),
            ),
            Err(_) => {
                self.engine
                    .commit_owned_data(commit)
                    .map_err(|_| failure(TunnelError::Engine, None))?;
                return Err(failure(TunnelError::InvalidWirePacket, None));
            }
        };
        let identity = Arc::new(());
        self.prepared_data = Some(PendingData {
            identity: Arc::downgrade(&identity),
            commit,
        });
        Ok(PreparedReceive::Data(PreparedData {
            identity,
            packet: plain,
            peer: None,
        }))
    }
    /// Requires this exact pending operation and rechecks the actual SDK
    /// Session/replay. Stack preparation/Drop is never accepted as a commit.
    pub fn commit_prepared_data(&mut self, identity: &Arc<()>) -> Result<(), TunnelError> {
        if !self
            .prepared_data
            .as_ref()
            .and_then(|p| p.identity.upgrade())
            .is_some_and(|same| Arc::ptr_eq(&same, identity))
        {
            return Err(TunnelError::Engine);
        }
        let pending = self.prepared_data.take().expect("same pending Data token");
        self.engine
            .commit_owned_data(pending.commit)
            .map_err(|_| TunnelError::Engine)
    }
}
