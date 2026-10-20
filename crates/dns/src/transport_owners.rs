//! DNS-owned persistent resources survive resolver publication and caller drop.
//! This registry is state-only at construction and never creates a runtime.
#[cfg(feature = "doh")]
use crate::backends::DohDriverOwner;
use std::io;
#[cfg(feature = "doh")]
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub(crate) struct TransportOwners {
    #[cfg(feature = "doh")]
    doh: Mutex<DohOwners>,
}
#[cfg(feature = "doh")]
#[derive(Default)]
struct DohOwners {
    closing: bool,
    owners: Vec<Arc<DohDriverOwner>>,
}
impl TransportOwners {
    #[cfg(feature = "doh")]
    pub(crate) fn register_doh(&self, owner: Arc<DohDriverOwner>) -> io::Result<()> {
        let mut state = self.doh.lock().expect("DNS DoH owners poisoned");
        if state.closing {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DNS DoH resources are closing",
            ));
        }
        state
            .owners
            .retain(|owner| Arc::strong_count(owner) > 1 || !owner.drained());
        state.owners.push(owner);
        Ok(())
    }
    pub(crate) async fn shutdown_doh(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        #[cfg(feature = "doh")]
        {
            let owners = {
                let mut state = self.doh.lock().expect("DNS DoH owners poisoned");
                state.closing = true;
                for owner in &state.owners {
                    owner.retire();
                }
                state.owners.clone()
            };
            let mut failure = None;
            for owner in owners {
                if let Err(source) = owner.shutdown(deadline).await {
                    failure.get_or_insert(source);
                }
            }
            return match failure {
                Some(source) => Err(source),
                None => Ok(()),
            };
        }
        #[cfg(not(feature = "doh"))]
        {
            let _ = deadline;
            Ok(())
        }
    }
}
