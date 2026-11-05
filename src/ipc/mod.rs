pub mod client;
mod connection;
mod events;
pub(crate) mod owner;
pub mod protocol;

#[cfg(unix)]
mod unix;
#[cfg(windows)]
pub(crate) mod windows;

// Re-export platform-specific items.
#[cfg(unix)]
pub use unix::{
    default_socket_path as default_ipc_path, prepare_ipc_server, prepare_ipc_server_with_budget,
    resolve_socket_path as resolve_ipc_path, spawn_ipc_server, IpcServerHandle,
};
#[cfg(windows)]
pub use windows::{
    default_socket_path as default_ipc_path, prepare_ipc_server, prepare_ipc_server_with_budget,
    resolve_socket_path as resolve_ipc_path, spawn_ipc_server, IpcServerHandle,
};
