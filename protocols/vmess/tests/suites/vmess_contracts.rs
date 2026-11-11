//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../cipher_names.rs"]
mod cipher_names;
#[cfg(feature = "runtime")]
#[path = "../component/mux_datagram.rs"]
mod mux_datagram;
#[cfg(feature = "runtime")]
#[path = "../tcp_stream.rs"]
mod tcp_stream;
