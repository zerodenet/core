//! Two actual directions of one request, retained by its returned body.
//! A logical stream/reset/error is not a physical driver or Scope failure.
use super::*;
use std::sync::OnceLock;
use zero_traits::packet_metadata::{PacketMetadataCredit, PacketMetadataError};
pub(super) struct RequestCauses {
    slots: [OnceLock<PacketMetadataError>; 2],
    memory: [std::sync::Mutex<Option<PacketMetadataCredit>>; 2],
}
impl RequestCauses {
    pub fn prepare(memory: &PacketDriverMetadataView) -> io::Result<PacketMetadataArc<Self>> {
        let header = PacketMetadataArc::<Self>::layout().map_err(io::Error::other)?;
        let error = PacketMetadataArc::<io::Error>::layout().map_err(io::Error::other)?;
        let mut paid = memory
            .reserve(
                header
                    .checked_add(error)
                    .and_then(|total| total.checked_add(error))
                    .map_err(io::Error::other)?,
            )
            .map_err(io::Error::other)?;
        let header_credit = paid.split(header).map_err(io::Error::other)?;
        let first = paid.split(error).map_err(io::Error::other)?;
        PacketMetadataArc::from_prepaid(
            Self {
                slots: [OnceLock::new(), OnceLock::new()],
                memory: [
                    std::sync::Mutex::new(Some(first)),
                    std::sync::Mutex::new(Some(paid)),
                ],
            },
            header_credit,
        )
        .map_err(|(source, _, _)| io::Error::other(source))
    }
    pub fn record(&self, direction: usize, error: io::Error) -> PacketMetadataError {
        if let Some(original) = self.slots[direction].get() {
            return original.clone();
        }
        let credit = self.memory[direction]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("one actual H3 request direction result");
        let original = PacketMetadataArc::from_prepaid(error, credit)
            .unwrap_or_else(|_| panic!("H3 original error header prepaid"))
            .into_error();
        assert!(self.slots[direction].set(original.clone()).is_ok());
        original
    }
    pub fn get(&self, direction: usize) -> Option<&PacketMetadataError> {
        self.slots.get(direction)?.get()
    }
}

#[derive(Debug)]
pub(super) struct Status(pub &'static str, pub http::StatusCode);
impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "xhttp {} status {}", self.0, self.1)
    }
}
impl std::error::Error for Status {}
