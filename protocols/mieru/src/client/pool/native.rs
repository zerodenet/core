//! Native TCP uses only its actual owner's granted background prefix.
use super::{ClientConnection, ClientPool, PoolKey};
use std::io;
use tokio::time::Instant;
use zero_core::{component::ComponentInstanceId, request::Destination, Address};
use zero_platform_tokio::contract::network::CarrierContext;

impl ClientPool {
    /// Library-only entry. No replacement pool, caller opener or business session
    /// can replace this pool's persistent, host-granted background carrier.
    pub async fn open_managed_tcp(
        &self,
        owner: ComponentInstanceId,
        mut key: PoolKey,
        options: &mieru_config::MieruTransportOptions,
        deadline: Option<Instant>,
    ) -> io::Result<crate::client::NativeTcpStream> {
        options.validate().map_err(io::Error::other)?;
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(io::ErrorKind::TimedOut.into());
        }
        if key.udp || key.server.is_empty() || key.port == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native Mieru TCP requires a nonempty TCP endpoint",
            ));
        }
        let state = self.managed(owner)?;
        if let Some(deadline) = deadline {
            if !state
                .retire_finished(deadline)
                .await
                .map_err(io::Error::other)?
            {
                return Err(io::Error::new(
                    if Instant::now() >= deadline {
                        io::ErrorKind::TimedOut
                    } else {
                        io::ErrorKind::WouldBlock
                    },
                    "Mieru retiring carrier has no completed close receipt",
                ));
            }
            if Instant::now() >= deadline {
                return Err(io::ErrorKind::TimedOut.into());
            }
        }
        // Do not trust a caller's legacy profile to isolate native carriers.
        key.profile = format!(
            "native:{owner:?}:{:?}:{options:?}",
            state.upstream.binding()
        );
        key.generation = state.upstream.binding().network_generation.0;
        let address = match zero_core::address::parse_ip_literal(&key.server) {
            Some(std::net::IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
            Some(std::net::IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
            None => Address::Domain(key.server.clone()),
        };
        let endpoint = Destination::new(address, key.port);
        let username = key.username.clone();
        let password = key.password.clone();
        let opening=self.open_with_connection(key, || {
            let lease = state.reserve();
            let state = state.clone();
            let endpoint=endpoint.clone();
            let username = username.clone();
            let password = password.clone();
            async move {
                let lease=lease?;
                if let Some(grants)=&state.grants {
                    let grant=grants.reserve().map_err(io::Error::other)?;
                    let adoption=grant.adoption();lease.adopt(adoption.clone()).map_err(io::Error::other)?;
                    lease.attempting_io();
                    let prepared=state.upstream.prepare_controlled_stream(endpoint,CarrierContext {session:None,deadline,cancel:state.cancel.clone()},grant)
                        .map_err(io::Error::other)?;
                    // The existing Mieru initialization bound is additional to
                    // the native opener's own (possibly stricter) connect bound.
                    let end=deadline.unwrap_or_else(||Instant::now()+std::time::Duration::from_secs(15));
                    let ready=tokio::select! {biased;
                        _=state.cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
                        _=state.scope_cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
                        result=prepared.open(end)=>result.map_err(io::Error::other)?,
                    };
                    let (io,_)=ready.into_parts();
                    return ClientConnection::tcp_controlled(io,adoption,&username,&password,options,lease).await;
                }
                #[cfg(test)]
                {
                lease.attempting_io();
                let socket = tokio::select! {biased;
                    _=state.cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
                    _=state.scope_cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
                    result=state.upstream.open_stream(endpoint,CarrierContext {session:None,deadline,cancel:state.cancel.clone()})=>result.map_err(io::Error::other)?,
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
        connection.native_stream(stream)
    }
}
