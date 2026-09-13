//! HTTP/3 carrier for the same XHTTP request/session contract as H1 and H2.
pub(super) mod client;
mod server;
pub use client::connect_xhttp_h3;
pub use server::accept_xhttp_h3_connection;
