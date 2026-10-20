//! Explicit native UDP root capability; the original HostResources owns controls.
use super::*;
use std::sync::Arc;
use zero_platform_tokio::contract::component::{
    RootDatagramAttemptFailure, RootDatagramClosed, RootDatagramGrant, RootDatagramIo,
};

pub(crate) struct OwnedDnsUdp {
    grants: Vec<RootDatagramGrant>,
}
impl OwnedDnsUdp {
    pub(crate) fn new(grants: Vec<RootDatagramGrant>) -> io::Result<Arc<Self>> {
        let first = grants.first().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "owned DNS UDP requires explicit root grants",
            )
        })?;
        for (index, grant) in grants.iter().enumerate() {
            if !first.same_root(grant)
                || first.network_generation() != grant.network_generation()
                || grants[..index]
                    .iter()
                    .any(|previous| previous.peer() == grant.peer())
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "DNS UDP grants must share one actual root/generation with unique exact peers",
                ));
            }
        }
        Ok(Arc::new(Self { grants }))
    }
    pub(crate) fn validate(&self, peers: &[SocketAddr]) -> io::Result<()> {
        if peers
            .iter()
            .all(|peer| self.grants.iter().any(|grant| grant.peer() == *peer))
        {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "DNS UDP endpoint has no exact original root grant",
            ))
        }
    }
    pub(crate) async fn exchange(
        self: &Arc<Self>,
        peers: &[SocketAddr],
        query: &[u8],
    ) -> OwnedUdpAttempt {
        let witness = match crate::OwnedDnsQueryTasks::current_witness() {
            Ok(witness) => witness,
            Err(error) => return OwnedUdpAttempt::Unsettled(error),
        };
        if let Err(error) = self.validate(peers) {
            return OwnedUdpAttempt::Unsettled(error);
        }
        let deadline = crate::backends::ATTEMPT_DEADLINE
            .try_with(|deadline| *deadline)
            .unwrap_or_else(|_| tokio::time::Instant::now() + DNS_TIMEOUT);
        let mut last = None;
        for (index, peer) in peers.iter().enumerate() {
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            let grant = self
                .grants
                .iter()
                .find(|grant| grant.peer() == *peer)
                .expect("validated exact peer");
            let opened = match grant.open(*peer, grant.network_generation(), &witness) {
                Ok(opened) => opened,
                Err(failure) => {
                    let permits = peers
                        .get(index + 1)
                        .and_then(|next| self.grants.iter().find(|grant| grant.peer() == *next))
                        .is_some_and(|next| {
                            failure
                                .retry_receipt()
                                .is_some_and(|receipt| receipt.permits(next, &witness))
                        });
                    let attempt = OwnedUdpAttempt::Acquisition {
                        failure,
                        root: self.clone(),
                    };
                    if !permits {
                        return attempt;
                    }
                    last = Some(attempt);
                    continue;
                }
            };
            // Enforce the existing backend deadline inside the actual socket
            // owner, then synchronously close its original before returning the
            // timeout. An outer timeout must not drop this close continuation.
            let result = tokio::time::timeout_at(
                deadline,
                exchange_on(UdpIo::Owned(&opened.io), *peer, query),
            )
            .await
            .unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "DNS backend attempt timed out",
                ))
            });
            let closed = match grant.close(opened.io) {
                Ok(closed) => closed,
                Err(source) => return OwnedUdpAttempt::Unsettled(io::Error::other(source)),
            };
            let succeeded = result.is_ok();
            let attempt = OwnedUdpAttempt::Closed {
                result,
                _closed: closed,
                root: self.clone(),
            };
            if succeeded {
                return attempt;
            }
            last = Some(attempt);
        }
        last.unwrap_or_else(|| {
            OwnedUdpAttempt::Unsettled(io::Error::new(
                io::ErrorKind::TimedOut,
                "DNS UDP attempt deadline elapsed before acquisition",
            ))
        })
    }
}

/// Retain actual disposition through the backend-policy boundary. No diagnostic
/// error-chain downcast, error kind, raw code or caller boolean grants replay.
pub(crate) enum OwnedUdpAttempt {
    Closed {
        result: io::Result<Vec<u8>>,
        _closed: RootDatagramClosed,
        root: Arc<OwnedDnsUdp>,
    },
    Acquisition {
        failure: RootDatagramAttemptFailure,
        root: Arc<OwnedDnsUdp>,
    },
    Unsettled(io::Error),
}
impl OwnedUdpAttempt {
    pub(crate) fn into_parts(
        self,
        next: Option<&crate::backends::ResolverBackend>,
    ) -> (io::Result<Vec<u8>>, bool) {
        let next = next
            .and_then(crate::backends::ResolverBackend::owned_udp)
            .and_then(UdpDnsResolver::owned_peer);
        match self {
            Self::Closed {
                result,
                _closed,
                root,
            } => {
                let permits = next.is_some_and(|(next_root, _)| Arc::ptr_eq(&root, next_root));
                (result, permits)
            }
            Self::Acquisition { failure, root } => {
                let permits = next.is_some_and(|(next_root, peer)| {
                    Arc::ptr_eq(&root, next_root)
                        && root
                            .grants
                            .iter()
                            .find(|grant| grant.peer() == peer)
                            .is_some_and(|next| {
                                crate::OwnedDnsQueryTasks::current_witness()
                                    .ok()
                                    .is_some_and(|query| {
                                        failure
                                            .retry_receipt()
                                            .is_some_and(|receipt| receipt.permits(next, &query))
                                    })
                            })
                });
                (Err(io::Error::other(failure.into_error())), permits)
            }
            Self::Unsettled(source) => (Err(source), false),
        }
    }
}

pub(super) enum UdpIo<'a> {
    Legacy(&'a TokioDatagramSocket),
    Owned(&'a RootDatagramIo),
}
impl UdpIo<'_> {
    pub(super) async fn send(&self, query: &[u8], peer: SocketAddr) -> io::Result<usize> {
        match self {
            Self::Legacy(io) => io.send_to_addr(query, peer).await,
            Self::Owned(io) => io.send_to_addr(query, peer).await,
        }
    }
    pub(super) async fn recv(&self, buffer: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        match self {
            Self::Legacy(io) => io.recv_from_addr(buffer).await,
            Self::Owned(io) => io.recv_from_addr(buffer).await,
        }
    }
}
