//! DNS-owned persistent resources survive resolver publication and caller drop.
//! This registry is state-only at construction and never creates a runtime.
#[cfg(feature = "doh")]
mod ech;
#[cfg(feature = "doh")]
use crate::backends::DohDriverOwner;
#[cfg(feature = "doq")]
use crate::backends::DoqEndpointOwner;
use std::io;
#[cfg(any(feature = "doh", feature = "doq"))]
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Default)]
pub(crate) struct TransportOwners {
    system: Mutex<Vec<crate::system::TokioSystemResolver>>,
    #[cfg(feature = "doh")]
    doh: Mutex<DohOwners>,
    #[cfg(feature = "doq")]
    doq: Mutex<DoqOwners>,
}
#[cfg(feature = "doh")]
#[derive(Default)]
struct DohOwners {
    endpoints: Vec<(
        crate::ech::pool::DohKey,
        Arc<crate::backends::ResolverBackend>,
    )>,
    closing: bool,
    owners: Vec<Arc<DohDriverOwner>>,
}
#[cfg(feature = "doq")]
#[derive(Default)]
struct DoqOwners {
    closing: bool,
    owners: Vec<Arc<DoqEndpointOwner>>,
}
impl TransportOwners {
    pub(crate) async fn shutdown_host_pools(
        &self,
        tasks: &crate::OwnedDnsQueryTasks,
        end: tokio::time::Instant,
    ) -> io::Result<()> {
        #[cfg(feature = "doh")]
        {
            let owners = self
                .doh
                .lock()
                .expect("DNS DoH owners poisoned")
                .owners
                .iter()
                .filter(|owner| owner.bound_to(tasks))
                .cloned()
                .collect::<Vec<_>>();
            let mut failure = None;
            for owner in &owners {
                owner.retire();
            }
            for owner in owners {
                if let Err(source) = owner.shutdown(end).await {
                    failure.get_or_insert(source);
                }
            }
            failure.map_or(Ok(()), Err)
        }
        #[cfg(not(feature = "doh"))]
        {
            let _ = (tasks, end);
            Ok(())
        }
    }

    pub(crate) fn register_system(
        &self,
        resolver: crate::system::TokioSystemResolver,
    ) -> io::Result<()> {
        let mut owners = self.system.lock().expect("DNS system owners poisoned");
        owners.retain(|owner| owner.externally_owned() || owner.retained());
        owners.push(resolver);
        Ok(())
    }
    pub(crate) async fn shutdown_system(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        let owners = self
            .system
            .lock()
            .expect("DNS system owners poisoned")
            .clone();
        for owner in &owners {
            owner.revoke();
        }
        let mut failure = None;
        for owner in owners {
            if let Err(source) = owner.shutdown(deadline).await {
                failure.get_or_insert(source);
            }
        }
        failure.map_or(Ok(()), Err)
    }
    #[cfg(feature = "doq")]
    pub(crate) fn register_doq(&self, owner: Arc<DoqEndpointOwner>) -> io::Result<()> {
        let mut state = self.doq.lock().expect("DNS DoQ owners poisoned");
        if state.closing {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DNS DoQ resources are closing",
            ));
        }
        state
            .owners
            .retain(|owner| Arc::strong_count(owner) > 1 || !owner.drained());
        state.owners.push(owner);
        Ok(())
    }
    #[cfg(feature = "doq")]
    pub(crate) async fn drain_retired_doq(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        let owners = self
            .doq
            .lock()
            .expect("DNS DoQ owners poisoned")
            .owners
            .iter()
            .filter(|owner| owner.closing())
            .cloned()
            .collect::<Vec<_>>();
        for owner in owners {
            owner.shutdown(deadline).await?;
        }
        Ok(())
    }
    pub(crate) async fn shutdown_doq(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        #[cfg(feature = "doq")]
        {
            let owners = {
                let mut state = self.doq.lock().expect("DNS DoQ owners poisoned");
                state.closing = true;
                for owner in &state.owners {
                    owner.request_close();
                }
                state.owners.clone()
            };
            let mut failure = None;
            for owner in owners {
                if let Err(source) = owner.shutdown(deadline).await {
                    failure.get_or_insert(source);
                }
            }
            match failure {
                Some(source) => Err(source),
                None => Ok(()),
            }
        }
        #[cfg(not(feature = "doq"))]
        {
            let _ = deadline;
            Ok(())
        }
    }
    #[cfg(all(test, feature = "doq"))]
    pub(crate) fn doq_snapshot(&self) -> Vec<Arc<DoqEndpointOwner>> {
        self.doq
            .lock()
            .expect("DNS DoQ owners poisoned")
            .owners
            .clone()
    }
    #[cfg(all(test, feature = "doh"))]
    pub(crate) fn doh_snapshot(&self) -> Vec<Arc<DohDriverOwner>> {
        self.doh
            .lock()
            .expect("DNS DoH owners poisoned")
            .owners
            .clone()
    }
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
            match failure {
                Some(source) => Err(source),
                None => Ok(()),
            }
        }
        #[cfg(not(feature = "doh"))]
        {
            let _ = deadline;
            Ok(())
        }
    }
}
