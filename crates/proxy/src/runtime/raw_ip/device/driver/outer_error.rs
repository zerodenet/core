//! Classification of outer raw-IP wire datagram errors during network changes.
use std::{error::Error, io};
use zero_platform_tokio::contract::{ChannelError, DeliveryState, Reason};

/// Returns `true` when an outer wire send failed before OS submission because
/// its bound egress generation, source address, or interface became stale.
pub(super) fn is_unsubmitted_stale_wire_error(error: &io::Error) -> bool {
    let mut current: Option<&(dyn Error + 'static)> =
        error.get_ref().map(|inner| inner as &(dyn Error + 'static));
    for _ in 0..32 {
        let Some(source) = current else {
            return false;
        };
        if let Some(channel) = source.downcast_ref::<ChannelError>() {
            return channel.delivery == DeliveryState::NotSubmitted
                && matches!(
                    channel.reason,
                    Reason::StalePlan | Reason::Revoked | Reason::Cancelled
                );
        }
        if let Some(nested_io) = source.downcast_ref::<io::Error>() {
            if let Some(inner) = nested_io.get_ref() {
                current = Some(inner);
                continue;
            }
        }
        current = source.source();
    }
    false
}

/// Returns `true` for per-datagram OS network errors commonly observed during
/// Wi-Fi / interface / route transitions (`ENETDOWN`, `ENETUNREACH`,
/// `EHOSTUNREACH`, `EADDRNOTAVAIL`, `ECONNREFUSED`, `ECONNRESET`, `ENOBUFS`).
pub(super) fn is_transient_outer_network_error(error: &io::Error) -> bool {
    zero_platform_tokio::is_transient_datagram_io_error(error)
}
