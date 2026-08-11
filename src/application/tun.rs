use std::error::Error;

use crate::cli::Command;
use crate::ipc::protocol::IpcRequest;

#[cfg(test)]
mod tests;

pub fn execute(command: Command) -> Result<(), Box<dyn Error>> {
    match command {
        Command::TunStart {
            name,
            addr,
            mask,
            secondary_addr,
            mtu,
            tag,
            auto_route,
            dual_stack,
            strict_route,
            dns_hijack,
            socket_path,
        } => {
            let request = IpcRequest::Command {
                id: None,
                method: "tun.start".to_owned(),
                params: serde_json::json!({
                    "name": name,
                    "addr": addr,
                    "mask": mask.unwrap_or_else(|| "255.255.255.0".to_owned()),
                    "secondary_addr": secondary_addr,
                    "mtu": mtu,
                    "tag": tag,
                    "auto_route": auto_route,
                    "dual_stack": dual_stack,
                    "strict_route": strict_route,
                    "dns_hijack": dns_hijack,
                }),
            };
            send_command(socket_path.as_deref(), request, "tun started")
        }
        Command::TunStop { socket_path } => send_command(
            socket_path.as_deref(),
            IpcRequest::Command {
                id: None,
                method: "tun.stop".to_owned(),
                params: serde_json::json!({}),
            },
            "tun stopped",
        ),
        Command::TunStatus { socket_path } => {
            let socket = super::resolve_socket(socket_path.as_deref())?;
            let response = crate::ipc::client::send_request(
                &socket,
                &IpcRequest::Query {
                    id: None,
                    request: zero_api::QueryRequest::TunStatus(zero_api::TunStatusQuery),
                },
            )?;
            if !response.ok {
                return Err(response
                    .error
                    .map(|error| error.message)
                    .unwrap_or_default()
                    .into());
            }
            let status = decode_tun_status(response.result.unwrap_or_default())?;
            if status.running {
                println!(
                    "tun: running, healthy={}, managed_by_config={}, name={}, addr={}, addresses={}, mtu={}, tag={}, auto_route={}, dual_stack={}, strict_route={}, dns_hijack={}, egress={}, egress_v4={}, egress_v6={}",
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
                    status.dual_stack,
                    status.strict_route,
                    status.dns_hijack,
                    status.egress_interface.as_deref().unwrap_or("-"),
                    status.egress_interface_v4.as_deref().unwrap_or("-"),
                    status.egress_interface_v6.as_deref().unwrap_or("-")
                );
            } else {
                if let Some(error) = status.last_error {
                    println!("tun: not running, last_error={error}");
                } else {
                    println!("tun: not running");
                }
            }
            Ok(())
        }
        _ => unreachable!("application routes only tun commands here"),
    }
}

fn decode_tun_status(
    value: serde_json::Value,
) -> Result<zero_api::TunStatusSnapshot, Box<dyn Error>> {
    let response: zero_api::QueryResponse = serde_json::from_value(value)?;
    let zero_api::QueryResponse::TunStatus(status) = response else {
        return Err("unexpected response to TUN status query".into());
    };
    Ok(status)
}

fn send_command(
    socket_path: Option<&str>,
    request: IpcRequest,
    success: &str,
) -> Result<(), Box<dyn Error>> {
    let socket = super::resolve_socket(socket_path)?;
    let response = crate::ipc::client::send_request(&socket, &request)?;
    if response.ok {
        println!("{success}");
        Ok(())
    } else {
        Err(response
            .error
            .map(|error| error.message)
            .unwrap_or_default()
            .into())
    }
}
