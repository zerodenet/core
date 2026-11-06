//! One concrete native request, executed by the same already-admitted SDK task.
use super::*;
use crate::TunDevice;
use std::net::IpAddr;
pub enum OwnedWintunRequest {
    Create {
        name: Option<String>,
        addresses: Vec<(IpAddr, IpAddr)>,
        mtu: u16,
    },
    Existing {
        interface: String,
        addresses: Vec<IpAddr>,
        mtu: u16,
    },
}
pub(super) struct Acquisition {
    pub(super) request: OwnedWintunRequest,
    pub(super) result: tokio::sync::oneshot::Sender<Result<OwnedWintunIo, Arc<io::Error>>>,
}
struct PendingAcquisition {
    owner: Arc<OwnedWintunOwner>,
    received: bool,
}
impl Drop for PendingAcquisition {
    fn drop(&mut self) {
        if !self.received {
            self.owner.stop();
        }
    }
}
impl OwnedWintunOwner {
    /// The actual host installs this request only after registering ReadWork and
    /// retaining that same blocking task's witness. Cancelling this await leaves
    /// the original request/native handles with this owner and requests stop.
    pub async fn acquire_in_reader(
        self: &Arc<Self>,
        request: OwnedWintunRequest,
    ) -> io::Result<OwnedWintunIo> {
        let (result, received) = tokio::sync::oneshot::channel();
        {
            let mut state = self.state();
            if state.attempted || self.closing.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "same Wintun acquisition already attempted or stopped",
                ));
            }
            let reader = state.prepared_reader.as_mut().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "actual SDK reader is not installed",
                )
            })?;
            if reader.acquire_queued || reader.done {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "same SDK acquisition receipt already assigned",
                ));
            }
            reader.acquire_queued = true;
            reader.acquisition = Some(Acquisition { request, result });
        }
        let mut pending = PendingAcquisition {
            owner: self.clone(),
            received: false,
        };
        self.reader_control
            .get()
            .expect("same prepared SDK control")
            .signal_input()?;
        let result = received
            .await
            .map_err(|source| io::Error::new(io::ErrorKind::BrokenPipe, source))?;
        pending.received = true;
        result.map_err(shared)
    }
}
impl OwnedWintunRequest {
    pub(super) fn execute(
        self,
        owner: &Arc<OwnedWintunOwner>,
    ) -> Result<OwnedWintunIo, Arc<io::Error>> {
        let result = (|| match self {
            Self::Create {
                name,
                addresses,
                mtu,
            } => {
                if addresses.is_empty()
                    || mtu == 0
                    || addresses
                        .iter()
                        .any(|(address, mask)| address.is_ipv4() != mask.is_ipv4())
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid native Wintun create request",
                    ));
                }
                let view = owner.create(name.as_deref())?;
                super::super::super::configure_adapter_until(
                    view.name(),
                    &addresses,
                    mtu,
                    Some(&owner.closing),
                )?;
                Ok(view)
            }
            Self::Existing {
                interface,
                addresses,
                mtu,
            } => owner.open_existing(&interface, &addresses, mtu),
        })();
        result.map_err(|source| {
            let existing = source
                .get_ref()
                .and_then(|source| source.downcast_ref::<super::super::SharedSource>())
                .map(|source| source.0.clone());
            let original = existing.unwrap_or_else(|| Arc::new(source));
            owner.state().acquisition_failure = Some(original.clone());
            original
        })
    }
}

#[cfg(test)]
mod tests;
