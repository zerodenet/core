//! Native TCP uses only its actual owner's granted background prefix.
use super::{ClientConnection, ClientPool, PoolKey};
use std::io;
use tokio::time::Instant;
use zero_core::{component::ComponentInstanceId, request::Destination, Address};
use zero_platform_tokio::contract::network::CarrierContext;
mod connect;
mod pending;

impl ClientPool {
    /// Library-only entry. No replacement pool, caller opener or business session
    /// can replace this pool's persistent, host-granted background carrier.
    pub async fn open_managed_tcp(
        &self,
        owner: ComponentInstanceId,
        key: PoolKey,
        options: &mieru_config::MieruTransportOptions,
        deadline: Option<Instant>,
    ) -> io::Result<crate::client::NativeTcpStream> {
        if key.udp {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TCP underlay required",
            ));
        }
        self.open_managed(owner, key, options, deadline).await
    }
    /// Both packet and stream underlays share the actual owner and total budget.
    pub async fn open_managed(
        &self,
        owner: ComponentInstanceId,
        key: PoolKey,
        options: &mieru_config::MieruTransportOptions,
        deadline: Option<Instant>,
    ) -> io::Result<crate::client::NativeTcpStream> {
        self.open_managed_via(owner, key, options, None, deadline)
            .await
    }
    /// The persistent host provider validates this selected binding and rebuilds
    /// its actual path. A caller cannot replace that provider with its opener.
    pub async fn open_managed_via(
        &self,
        owner: ComponentInstanceId,
        key: PoolKey,
        options: &mieru_config::MieruTransportOptions,
        binding: Option<zero_platform_tokio::contract::network::UpstreamBinding>,
        deadline: Option<Instant>,
    ) -> io::Result<crate::client::NativeTcpStream> {
        self.open_managed_via_observed(owner, key, options, binding, deadline, None)
            .await
    }
    /// Cleanup observation is weak and exists only for the actual waiting
    /// operation. The persistent provider still selects every real I/O path.
    pub async fn open_managed_via_observed(
        &self,
        owner: ComponentInstanceId,
        mut key: PoolKey,
        options: &mieru_config::MieruTransportOptions,
        binding: Option<zero_platform_tokio::contract::network::UpstreamBinding>,
        deadline: Option<Instant>,
        waiting: Option<zero_platform_tokio::contract::network::PendingCarrierObserver>,
    ) -> io::Result<crate::client::NativeTcpStream> {
        options.validate().map_err(io::Error::other)?;
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(io::ErrorKind::TimedOut.into());
        }
        if key.server.is_empty() || key.port == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native Mieru requires a nonempty endpoint",
            ));
        }
        let state = self.managed(owner)?;
        let fixed_user = if state.dynamic {
            None
        } else {
            Some(state.user()?)
        };
        let carrier = state.carrier(&key, options)?;
        let upstream = if state.dynamic {
            carrier
                .upstream
                .select_background_carrier(
                    binding,
                    CarrierContext {
                        session: None,
                        deadline,
                        cancel: state.cancel.clone(),
                    },
                )
                .await
                .map_err(io::Error::other)?
        } else {
            if binding
                .as_ref()
                .is_some_and(|binding| binding != carrier.upstream.binding())
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Mieru selected binding differs from the actual fixed provider",
                ));
            }
            carrier.upstream.clone()
        };
        let grants = carrier.grants.clone();
        let provider = carrier.upstream.clone();
        // An omitted caller deadline must not skip retirement forever. Reuse
        // Mieru's existing initialization bound for this close attempt only.
        let retirement =
            deadline.unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(15));
        if !state
            .retire_finished(retirement)
            .await
            .map_err(io::Error::other)?
        {
            return Err(io::Error::new(
                if Instant::now() >= retirement {
                    io::ErrorKind::TimedOut
                } else {
                    io::ErrorKind::WouldBlock
                },
                "Mieru retiring carrier has no completed close receipt",
            ));
        }
        if deadline.is_some_and(|end| Instant::now() >= end) {
            return Err(io::ErrorKind::TimedOut.into());
        }
        // Do not trust a caller's legacy profile to isolate native carriers.
        let resolver_generation = upstream.carrier_dns_generation();
        let user = if let Some(user) = fixed_user {
            user
        } else {
            let epoch = (upstream.binding().network_generation, resolver_generation);
            match state.user_for_epoch(epoch)? {
                Some(user) => user,
                None => {
                    // Cancel the actual old workers and retain every original
                    // control until explicit close and transferred-user return.
                    self.clear();
                    state.retire_epoch();
                    if !state
                        .retire_finished(retirement)
                        .await
                        .map_err(io::Error::other)?
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::WouldBlock,
                            "Mieru old network originals remain unconfirmed",
                        ));
                    }
                    state.user_for_epoch(epoch)?.ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::WouldBlock,
                            "Mieru old network users remain retained",
                        )
                    })?
                }
            }
        };
        key.profile = format!(
            "native:{owner:?}:{:?}:{options:?}:{resolver_generation:?}",
            upstream.binding()
        );
        key.generation = upstream.binding().network_generation.0;
        let address = match zero_core::address::parse_ip_literal(&key.server) {
            Some(std::net::IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
            Some(std::net::IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
            None => Address::Domain(key.server.clone()),
        };
        let endpoint = Destination::new(address, key.port);
        let username = key.username.clone();
        let password = key.password.clone();
        let udp = key.udp;
        let opening=self.open_with_connection(key, || {
            let lease = state.reserve_with_pending_limit(upstream.binding().prefix.len());
            let state = state.clone();
            let upstream=upstream.clone();
            let grants=grants.clone();
            let provider=provider.clone();
            let waiting=waiting.clone();
            let endpoint=endpoint.clone();
            let username = username.clone();
            let password = password.clone();
            async move {
                let lease=lease?;
                let attempt_cancel=lease.cancellation();
                if let Some(grants)=&grants {
                    if !upstream.binding().prefix.is_empty() {
                        return connect::BorrowRequest {udp,upstream,provider,waiting,endpoint,username:&username,password:&password,options,deadline}.open(lease,grants).await;
                    }
                    let candidates = tokio::select! { biased;
                        _=attempt_cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
                        _=state.scope_cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
                        result=upstream.resolve_controlled_carrier(endpoint,CarrierContext {session:None,deadline,cancel:attempt_cancel.clone()})=>result.map_err(io::Error::other)?,
                    };
                    return connect::Request { candidates, username: &username, password: &password, options, deadline, udp, waiting }.open(lease, grants).await;
                }
                #[cfg(test)]
                {
                lease.attempting_io();
                let socket = tokio::select! {biased;
                    _=attempt_cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
                    _=state.scope_cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
                    result=upstream.open_stream(endpoint,CarrierContext {session:None,deadline,cancel:attempt_cancel.clone()})=>result.map_err(io::Error::other)?,
                };
                ClientConnection::tcp_managed(socket, &username, &password, options, lease).await
                }
                #[cfg(not(test))]
                {Err(io::Error::new(io::ErrorKind::Unsupported,"Mieru native Stream requires a controlled carrier owner"))}
            }
        });
        let (stream, connection) = match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, opening)
                .await
                .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?,
            None => opening.await,
        }?;
        if upstream.carrier_dns_generation() != resolver_generation {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "Mieru node DNS generation changed during open",
            ));
        }
        connection.native_stream(stream, user)
    }
}
