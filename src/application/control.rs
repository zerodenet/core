use std::error::Error;

use zero_api::{FlowListQuery, PoliciesQuery, QueryRequest};

use super::resolve_socket;
use crate::cli::Command;
use crate::ipc;

pub fn execute(command: Command) -> Result<(), Box<dyn Error>> {
    match command {
        Command::Select {
            policy_tag,
            target_tag,
            socket_path,
        } => select(&policy_tag, &target_tag, socket_path.as_deref()),
        Command::Flows { socket_path } => print_query(
            socket_path.as_deref(),
            QueryRequest::ActiveFlows(FlowListQuery {
                limit: Some(100),
                filter: Default::default(),
            }),
        ),
        Command::Policies { socket_path } => print_query(
            socket_path.as_deref(),
            QueryRequest::Policies(PoliciesQuery),
        ),
        Command::Events { socket_path } => events(socket_path.as_deref()),
        Command::Reload {
            config_path,
            socket_path,
        } => reload(&config_path, socket_path.as_deref()),
        Command::Mode {
            mode,
            outbound,
            socket_path,
        } => {
            let response = send_command(
                socket_path.as_deref(),
                "mode.set",
                serde_json::json!({"mode": mode, "outbound": outbound}),
            )?;
            ensure_ok(response)?;
            let target = outbound.map_or(mode, |tag| format!("global -> {tag}"));
            println!("mode switched to {target}");
            Ok(())
        }
        _ => unreachable!("application routes only control commands here"),
    }
}

fn select(
    policy_tag: &str,
    target_tag: &str,
    socket_path: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    ensure_ok(send_command(
        socket_path,
        "policies.select",
        serde_json::json!({"policy_tag": policy_tag, "target_tag": target_tag}),
    )?)?;
    println!("selector `{policy_tag}` -> `{target_tag}`");
    Ok(())
}

fn events(socket_path: Option<&str>) -> Result<(), Box<dyn Error>> {
    let socket = resolve_socket(socket_path)?;
    ipc::client::stream_events(
        &socket,
        &ipc::protocol::IpcRequest::Subscribe {
            id: None,
            events: None,
        },
        |event| println!("{}", serde_json::to_string(&event).unwrap_or_default()),
    )?;
    Ok(())
}

fn reload(config_path: &str, socket_path: Option<&str>) -> Result<(), Box<dyn Error>> {
    let raw = zero_runtime::configuration::ConfigurationSource::read_initial_document(config_path)?;
    let config: serde_json::Value = serde_json::from_str(&raw)?;
    ensure_ok(send_command(
        socket_path,
        "config.apply",
        serde_json::json!({"config": config}),
    )?)?;
    println!("config applied");
    Ok(())
}

fn print_query(socket_path: Option<&str>, request: QueryRequest) -> Result<(), Box<dyn Error>> {
    let socket = resolve_socket(socket_path)?;
    let response = ensure_ok(ipc::client::send_request(
        &socket,
        &ipc::protocol::IpcRequest::Query { id: None, request },
    )?)?;
    if let Some(result) = response.result {
        println!("{}", serde_json::to_string_pretty(&result)?);
    }
    Ok(())
}

fn send_command(
    socket_path: Option<&str>,
    method: &str,
    params: serde_json::Value,
) -> Result<ipc::protocol::IpcResponse, Box<dyn Error>> {
    let socket = resolve_socket(socket_path)?;
    Ok(ipc::client::send_request(
        &socket,
        &ipc::protocol::IpcRequest::Command {
            id: None,
            method: method.to_owned(),
            params,
        },
    )?)
}

fn ensure_ok(
    response: ipc::protocol::IpcResponse,
) -> Result<ipc::protocol::IpcResponse, Box<dyn Error>> {
    if response.ok {
        Ok(response)
    } else {
        Err(response
            .error
            .map(|error| error.message)
            .unwrap_or_else(|| "unknown IPC error".to_owned())
            .into())
    }
}
