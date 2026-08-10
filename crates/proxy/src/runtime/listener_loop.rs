#[cfg(feature = "transport_quic")]
mod quic;
mod system;
mod tcp;
#[cfg(test)]
mod tests;

#[cfg(feature = "transport_quic")]
pub(crate) use quic::{run_logged_quic_stream_listener_loop, LoggedQuicStreamListenerRequest};
#[cfg(feature = "authenticated-quic-inbound-runtime")]
pub(crate) use quic::{run_quic_listener_loop, QuicListenerLoopRequest};
#[cfg(all(test, feature = "transport_quic"))]
pub(crate) use quic::{run_quic_stream_listener_loop, QuicStreamListenerLoopRequest};
pub(crate) use system::{run_system_tcp_stack_loop, SystemTcpStackLoopRequest};
pub(crate) use tcp::{run_logged_tcp_socket_listener_loop, LoggedTcpSocketListenerRequest};
#[cfg(test)]
pub(crate) use tcp::{run_tcp_listener_loop, TcpListenerLoopRequest};
