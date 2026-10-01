//! Related integration cases share one executable; feature gates stay on cases.

#[cfg(feature = "runtime")]
#[path = "../support/socket.rs"]
mod socket;

#[path = "../legacy_replay.rs"]
mod legacy_replay;
#[cfg(feature = "runtime")]
#[path = "../outbound.rs"]
mod outbound;
#[path = "../reference_2022.rs"]
mod reference_2022;
#[path = "../reference_ciphers.rs"]
mod reference_ciphers;
#[path = "../replay_window.rs"]
mod replay_window;
#[cfg(feature = "runtime")]
#[path = "../shared.rs"]
mod shared;
#[path = "../tcp_accept_boundaries.rs"]
mod tcp_accept_boundaries;
#[cfg(feature = "runtime")]
#[path = "../user_profile.rs"]
mod user_profile;
