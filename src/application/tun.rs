use std::error::Error;

use crate::cli::Command;
use crate::ipc::protocol::IpcRequest;

#[cfg(test)]
mod tests;

pub async fn execute(
    command: Command,
    client: Option<&crate::ipc::client::Caller>,
) -> Result<(), Box<dyn Error>> {
    match command {
        Command::TunStart {
            name,
            addr,
            mask,
            secondary_addr,
            mtu,
            tag,
            auto_route,
            include_cidrs,
            exclude_cidrs,
            dual_stack,
            strict_route,
            dns_hijack,
            socket_path,
        } => {
            #[derive(serde::Serialize)]
            struct Params<'a> {
                name: Option<&'a str>,
                addr: &'a str,
                mask: &'a str,
                secondary_addr: Option<&'a str>,
                mtu: Option<u16>,
                tag: &'a str,
                auto_route: bool,
                include_cidrs: &'a [String],
                exclude_cidrs: &'a [String],
                dual_stack: bool,
                strict_route: bool,
                dns_hijack: bool,
            }
            send_command(
                client.expect("actual TUN client installed"),
                socket_path.as_deref(),
                "tun.start",
                Params {
                    name: name.as_deref(),
                    addr: &addr,
                    mask: mask.as_deref().unwrap_or("255.255.255.0"),
                    secondary_addr: secondary_addr.as_deref(),
                    mtu,
                    tag: &tag,
                    auto_route,
                    include_cidrs: &include_cidrs,
                    exclude_cidrs: &exclude_cidrs,
                    dual_stack,
                    strict_route,
                    dns_hijack,
                },
                "tun started",
            )
            .await
        }
        Command::TunRecover { socket_path } => {
            send_command(
                client.expect("actual TUN client installed"),
                socket_path.as_deref(),
                "tun.recover",
                Empty {},
                "tun network recovered",
            )
            .await
        }
        Command::TunStop { socket_path } => {
            send_command(
                client.expect("actual TUN client installed"),
                socket_path.as_deref(),
                "tun.stop",
                Empty {},
                "tun stopped",
            )
            .await
        }
        Command::TunStatus { socket_path } => {
            let socket = super::resolve_socket(socket_path.as_deref())?;
            let response = client
                .expect("actual TUN client installed")
                .request(
                    &socket,
                    &IpcRequest::Query {
                        id: None,
                        request: zero_api::QueryRequest::TunStatus(zero_api::TunStatusQuery),
                    },
                )
                .await?
                .ensure_ok("")?;
            let status = response.tun_status()?;
            if status.running {
                println!(
                    "tun: running, healthy={}, managed_by_config={}, name={}, addr={}, addresses={}, mtu={}, tag={}, auto_route={}, include_cidrs={}, exclude_cidrs={}, dual_stack={}, strict_route={}, dns_hijack={}, egress={}, egress_v4={}, egress_v6={}, ipv4_state={}, ipv4_reason={}, ipv6_state={}, ipv6_reason={}, address_family_policy={}, generation={}, ipv6_to_ipv4_fallbacks={}, last_error={}",
                    status.healthy,
                    status.managed_by_config,
                    status.name.as_deref().unwrap_or("-"),
                    status.addr.as_deref().unwrap_or("-"),
                    if status.addresses.is_empty() {
                        "-".to_owned()
                    } else {
                        status.addresses.join(",")
                    },
                    status
                        .mtu
                        .map(|mtu| mtu.to_string())
                        .as_deref()
                        .unwrap_or("-"),
                    status.tag.as_deref().unwrap_or("-"),
                    status.auto_route,
                    if status.include_cidrs.is_empty() {
                        "full-tunnel".to_owned()
                    } else {
                        status.include_cidrs.join(",")
                    },
                    if status.exclude_cidrs.is_empty() {
                        "-".to_owned()
                    } else {
                        status.exclude_cidrs.join(",")
                    },
                    status.dual_stack,
                    status.strict_route,
                    status.dns_hijack,
                    status.egress_interface.as_deref().unwrap_or("-"),
                    status.egress_interface_v4.as_deref().unwrap_or("-"),
                    status.egress_interface_v6.as_deref().unwrap_or("-"),
                    status.ipv4_egress.availability.as_str(),
                    status.ipv4_egress.reason.as_deref().unwrap_or("-"),
                    status.ipv6_egress.availability.as_str(),
                    status.ipv6_egress.reason.as_deref().unwrap_or("-"),
                    status.address_family_policy.as_deref().unwrap_or("-"),
                    status.network_generation,
                    status.ipv6_to_ipv4_fallbacks,
                    status.last_error.as_deref().unwrap_or("-")
                );
            } else if let Some(error) = status.last_error.as_ref() {
                println!("tun: not running, last_error={error}");
            } else {
                println!("tun: not running");
            }
            Ok(())
        }
        Command::MacosTunCreateHelper {
            socket_path,
            name,
            owned,
        } => {
            #[cfg(target_os = "macos")]
            {
                if owned {
                    zero_tun::run_owned_utun_create_helper(std::path::Path::new(&socket_path))?;
                } else {
                    zero_tun::run_utun_create_helper(
                        std::path::Path::new(&socket_path),
                        name.as_deref(),
                    )?;
                }
                Ok(())
            }
            #[cfg(not(target_os = "macos"))]
            {
                let _ = (socket_path, name, owned);
                Err("macOS TUN helper is unavailable on this platform".into())
            }
        }
        _ => unreachable!("application routes only tun commands here"),
    }
}

#[cfg(test)]
fn decode_tun_status(
    value: serde_json::Value,
) -> Result<zero_api::TunStatusSnapshot, Box<dyn Error>> {
    let response: zero_api::QueryResponse = serde_json::from_value(value)?;
    let zero_api::QueryResponse::TunStatus(status) = response else {
        return Err("unexpected response to TUN status query".into());
    };
    Ok(status)
}

#[derive(serde::Serialize)]
struct Empty {}
async fn send_command(
    client: &crate::ipc::client::Caller,
    socket_path: Option<&str>,
    method: &str,
    params: impl serde::Serialize,
    success: &str,
) -> Result<(), Box<dyn Error>> {
    let socket = super::resolve_socket(socket_path)?;
    client
        .request(&socket, &crate::ipc::client::Command::new(method, params))
        .await?
        .ensure_ok("")?;
    println!("{success}");
    Ok(())
}
