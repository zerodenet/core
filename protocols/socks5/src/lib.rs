#![no_std]
#![allow(async_fn_in_trait)]

extern crate alloc;
#[cfg(feature = "runtime")]
extern crate std;

#[cfg(feature = "runtime")]
pub mod component;
#[cfg(feature = "runtime")]
mod inbound;
mod metadata;
#[cfg(feature = "runtime")]
mod outbound;
#[cfg(feature = "runtime")]
mod shared;
#[cfg(feature = "runtime")]
pub mod transport;
#[cfg(feature = "runtime")]
pub mod udp;
#[cfg(feature = "validation")]
pub mod validation;

#[cfg(feature = "runtime")]
pub use inbound::{
    is_socks5_greeting_byte, ConfiguredSocks5PasswordAuth, ConfiguredSocks5User,
    IntoSocks5AuthUserConfig, NoSocks5PasswordAuth, Socks5Command, Socks5CommandFacts,
    Socks5Inbound, Socks5InboundTcpAcceptor, Socks5PasswordAuth, Socks5Request,
};
pub use metadata::Socks5Protocol;
#[cfg(feature = "runtime")]
pub use outbound::{
    Socks5Outbound, Socks5OutboundAuth, Socks5OwnedOutboundAuth, Socks5TcpConnectSpec,
    Socks5TcpOutboundProfile,
};
#[cfg(feature = "runtime")]
pub use shared::Socks5Reply;
#[cfg(feature = "validation")]
pub use validation::validate_credential_part;
