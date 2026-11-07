//! Original finite listener bind attempts and same-Scope resolver.
use super::*;
pub(super) async fn bind_control_listener(
    listen: &str,
    scope: &mut TaskScope,
    sockets: NonZeroUsize,
    per_entry: NonZeroUsize,
    slot: &tokio::sync::Mutex<Option<ControlledTcpListenerOwner>>,
) -> io::Result<ControlledTcpListener> {
    let attempts = NonZeroUsize::new(16).unwrap();
    let limits = AcceptedTcpLimits {
        max_sockets: sockets,
        max_tasks_per_socket: per_entry,
        max_resources_per_socket: 0,
    };
    let literal = listen.parse::<std::net::SocketAddr>();
    let (destination, addresses) = if let Ok(address) = literal {
        let host = match address.ip() {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        };
        (Destination::new(host, address.port()), vec![address])
    } else {
        let (host, port) = listen.rsplit_once(':').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "HTTP listen requires host:port",
            )
        })?;
        let port = port
            .parse::<u16>()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let requested = listen.to_owned();
        let material = Arc::new(std::sync::Mutex::new(None));
        let result = material.clone();
        scope
            .registrar()
            .spawn_blocking_tracked(TaskClass::Auxiliary, move || {
                use std::net::ToSocketAddrs;
                let addresses = requested
                    .to_socket_addrs()
                    .map(|addresses| addresses.take(17).collect::<Vec<_>>());
                *result.lock().unwrap() = Some(addresses);
                Ok(())
            })
            .map_err(io::Error::other)?;
        let exit = scope
            .next_exit()
            .await
            .expect("actual resolver registration");
        exit.result.map_err(io::Error::other)?;
        let addresses = material
            .lock()
            .unwrap()
            .take()
            .expect("actual completed resolver result")?;
        if addresses.is_empty() || addresses.len() > attempts.get() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "HTTP listener address set exceeds its declared attempts",
            ));
        }
        (
            Destination::new(Address::Domain(host.to_owned()), port),
            addresses,
        )
    };
    let declaration = ResourceDeclaration::TcpListener {
        destination,
        accepted: Some(limits),
        max_bind_attempts: attempts,
    };
    let mut owner = if literal.is_ok() {
        ControlledTcpListenerOwner::new_with_accepted_graph(
            &declaration,
            attempts,
            CancellationToken::new(),
            sockets,
            per_entry,
            &scope.registrar(),
        )
    } else {
        ControlledTcpListenerOwner::new_resolving_with_accepted_graph(
            &declaration,
            attempts,
            CancellationToken::new(),
            sockets,
            per_entry,
            &scope.registrar(),
        )
    }
    .map_err(io::Error::other)?;
    if literal.is_err() {
        owner
            .install_resolved_addresses(addresses.clone().into())
            .map_err(io::Error::other)?;
    }
    let mut slot = slot.lock().await;
    *slot = Some(owner);
    let owner = slot
        .as_mut()
        .expect("cold native owner installed before bind");
    let mut failure = None;
    for address in addresses {
        match owner.bind_listener(address) {
            Ok(listener) => return Ok(listener),
            Err(error) => {
                failure = Some(error);
                if owner.native_unconfirmed() {
                    break;
                }
            }
        }
    }
    let close = owner
        .wait_closed(crate::ipc::owner::close_deadline())
        .await
        .map_err(io::Error::other)?;
    if !close.is_complete() {
        return Err(io::Error::other("failed HTTP bind retains an original"));
    }
    Err(io::Error::other(
        failure.expect("nonempty frozen bind addresses"),
    ))
}
