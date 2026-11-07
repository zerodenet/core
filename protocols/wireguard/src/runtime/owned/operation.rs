//! Every owned operation returns its unused exact control allocation, including
//! failure. The original driver releases it outside its actual device lock.
use super::*;

pub struct OwnedTunnelResult<T> {
    pub result: Result<T, OwnedTunnelFailure>,
    pub unused_control: Option<PacketBuffer>,
}
impl PeerTunnel {
    pub(in crate::runtime) fn install_control(
        &mut self,
        control: PacketBuffer,
    ) -> Result<(), (PacketDeriveError, PacketBuffer)> {
        if control.len() < 148 {
            return Err((PacketDeriveError::LayoutMismatch, control));
        }
        let packet = Packet::from_owned_buffer(control)
            .map_err(|original| (PacketDeriveError::Unreserved, original))?;
        self.engine
            .install_owned_control(packet)
            .map_err(|packet| (PacketDeriveError::LayoutMismatch, owned_packet(packet)))
    }
    pub(in crate::runtime) fn finish_owned_operation<T>(
        &mut self,
        result: Result<T, OwnedTunnelFailure>,
    ) -> OwnedTunnelResult<T> {
        OwnedTunnelResult {
            result,
            unused_control: self.engine.take_unused_owned_control().map(owned_packet),
        }
    }
    pub fn receive_datagram_owned(
        &mut self,
        source: Option<SocketAddr>,
        mut datagram: PacketBuffer,
        decode: zero_tun::packet_memory::NativeReadBuffer,
        control: PacketBuffer,
    ) -> OwnedTunnelResult<ReceivedDatagram> {
        if let Err((storage, control)) = self.install_control(control) {
            return OwnedTunnelResult {
                result: Err(OwnedTunnelFailure {
                    error: TunnelError::Engine,
                    storage: Some(storage),
                    original: Some(datagram),
                }),
                unused_control: Some(control),
            };
        }
        // Handshake/COOKIE progress uses its original prepaid Native read,
        // never fresh global TX quota. Only a not-yet-submitted Data frame
        // may be rejected for TX capacity before replay/counter commitment.
        let data = super::is_data_datagram(&datagram);
        if data {
            if let Err(storage) = datagram.prepare_transmit(0, 0) {
                return self.finish_owned_operation(Err(OwnedTunnelFailure {
                    error: TunnelError::Engine,
                    storage: Some(storage),
                    original: Some(datagram),
                }));
            }
        }
        let result = match check_wire_datagram(&datagram) {
            Err(error) => Err(OwnedTunnelFailure {
                error,
                storage: None,
                original: Some(datagram),
            }),
            Ok(()) => {
                let original = Arc::new(datagram);
                match super::receive_scratch(&original, decode, data) {
                    Err(storage) => Err(OwnedTunnelFailure {
                        error: TunnelError::Engine,
                        storage: Some(storage),
                        original: Arc::try_unwrap(original).ok(),
                    }),
                    Ok(packet) => self
                        .receive_prepared_packet(source, packet)
                        .map_err(protocol_failure),
                }
            }
        };
        self.finish_owned_operation(result)
    }
    pub fn tick_owned(&mut self, control: PacketBuffer) -> OwnedTunnelResult<Vec<TunnelAction>> {
        if let Err((storage, control)) = self.install_control(control) {
            return OwnedTunnelResult {
                result: Err(OwnedTunnelFailure {
                    error: TunnelError::Engine,
                    storage: Some(storage),
                    original: None,
                }),
                unused_control: Some(control),
            };
        }
        let result = self.tick().map_err(protocol_failure);
        self.finish_owned_operation(result)
    }
    pub fn initiate_handshake_owned(
        &mut self,
        control: PacketBuffer,
    ) -> OwnedTunnelResult<Vec<TunnelAction>> {
        if let Err((storage, control)) = self.install_control(control) {
            return OwnedTunnelResult {
                result: Err(OwnedTunnelFailure {
                    error: TunnelError::Engine,
                    storage: Some(storage),
                    original: None,
                }),
                unused_control: Some(control),
            };
        }
        let result = self.initiate_handshake().map_err(protocol_failure);
        self.finish_owned_operation(result)
    }
}
fn protocol_failure(error: TunnelError) -> OwnedTunnelFailure {
    OwnedTunnelFailure {
        error,
        storage: None,
        original: None,
    }
}
