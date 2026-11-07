//! Same actual peer candidate loop, with replay commitment deferred until the
//! original consumer has admitted this exact decoded allocation.
use super::*;
use crate::runtime::{OwnedTunnelFailure, OwnedTunnelResult, PreparedReceive};
use zero_traits::PacketBuffer;
use zero_tun::packet_memory::{NativeReadBuffer, NativeReadGrant};
impl InboundDevice {
    pub fn prepare_datagram_owned(
        &mut self,
        source: Option<SocketAddr>,
        original: PacketBuffer,
        decode: NativeReadBuffer,
        decode_owner: &NativeReadGrant,
        control: PacketBuffer,
    ) -> OwnedTunnelResult<PreparedReceive<InboundDispatch>> {
        if !crate::runtime::owned::is_data_datagram(&original) {
            let output =
                self.receive_datagram_owned(source, original, decode, decode_owner, control);
            return OwnedTunnelResult {
                result: output.result.map(PreparedReceive::Complete),
                unused_control: output.unused_control,
            };
        }
        let receiver = original
            .get(4..8)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes);
        let initial = receiver.and_then(|receiver| self.local_indices.get(&receiver).copied());
        let mut decode = Some(decode);
        let mut last = OwnedTunnelFailure {
            error: TunnelError::UnknownPeer,
            storage: None,
            original: None,
        };
        for peer in initial
            .into_iter()
            .chain((0..self.peers.len()).filter(|peer| Some(*peer) != initial))
        {
            let scratch = match decode.take() {
                Some(scratch) => scratch,
                None => match decode_owner.try_buffer() {
                    Ok(scratch) => scratch,
                    Err(error) => {
                        return OwnedTunnelResult {
                            result: Err(OwnedTunnelFailure {
                                error: TunnelError::Engine,
                                storage: Some(owned::memory_error(error)),
                                original: Some(original),
                            }),
                            unused_control: Some(control),
                        }
                    }
                },
            };
            match self.peers[peer].prepare_data(&original, scratch) {
                Ok(PreparedReceive::Data(mut prepared)) => {
                    prepared.peer = Some(peer);
                    return OwnedTunnelResult {
                        result: Ok(PreparedReceive::Data(prepared)),
                        unused_control: Some(control),
                    };
                }
                Ok(PreparedReceive::Complete(())) => {
                    self.peer_sources[peer] = PeerSource {
                        last_authenticated_at: Some(Instant::now()),
                        endpoint: source,
                    };
                    return OwnedTunnelResult {
                        result: Ok(PreparedReceive::Complete(InboundDispatch {
                            peer_index: Some(peer),
                            authenticated: true,
                            source_rejected_packets: 0,
                            actions: Vec::new(),
                        })),
                        unused_control: Some(control),
                    };
                }
                Err(error) => {
                    last = error;
                    if last.storage.is_some() {
                        break;
                    }
                }
            }
        }
        last.original = Some(original);
        OwnedTunnelResult {
            result: Err(last),
            unused_control: Some(control),
        }
    }
    /// Same actual peer+pending token; this only commits software protocol state.
    /// The caller separately enforces authenticated AllowedIPs on the packet.
    pub fn commit_prepared_data(
        &mut self,
        peer: usize,
        identity: &Arc<()>,
        source: Option<SocketAddr>,
    ) -> Result<(), TunnelError> {
        let tunnel = self.peers.get_mut(peer).ok_or(TunnelError::UnknownPeer)?;
        tunnel.commit_prepared_data(identity)?;
        self.peer_sources[peer] = PeerSource {
            last_authenticated_at: Some(Instant::now()),
            endpoint: source,
        };
        Ok(())
    }
    pub fn prepared_source_allowed(&self, peer: usize, source: IpAddr) -> bool {
        self.profile
            .routes
            .allows_authenticated_source(peer, source)
    }
}
