use alloc::vec::Vec;
use prost::Message;
use rand::{Rng, RngCore};
use zero_core::Error;

pub const ACTIVE: i32 = 0;
pub const DRAIN: i32 = 1;

/// Unknown enum values are preserved, as in the reference protobuf decoder.
/// They do not make a bridge eligible for new sessions.
#[derive(Clone, PartialEq, Message)]
pub struct Control {
    #[prost(int32, tag = "1")]
    pub state: i32,
    #[prost(bytes = "vec", tag = "99")]
    pub random: Vec<u8>,
}

impl Control {
    pub fn heartbeat(state: i32) -> Self {
        let mut random = alloc::vec![0; rand::rng().random_range(1..=64)];
        rand::rng().fill_bytes(&mut random);
        Self { state, random }
    }

    pub fn from_packet(bytes: &[u8]) -> Result<Self, Error> {
        // One MUX UDP payload is independently framed by a u16 length.
        if bytes.len() > u16::MAX as usize {
            return Err(Error::Protocol("Rvs control packet is too large"));
        }
        Self::decode(bytes).map_err(|_| Error::Protocol("invalid Rvs control protobuf"))
    }

    pub fn into_packet(self) -> Vec<u8> {
        self.encode_to_vec()
    }
}
