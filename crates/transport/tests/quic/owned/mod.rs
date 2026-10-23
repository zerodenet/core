use super::*;
use std::{future::poll_fn, io, num::NonZeroUsize, sync::Arc};
use zero_platform_tokio::controlled_datagram::metadata::{MetadataLimits, MetadataOwner};
fn bound(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
fn native_owner() -> MetadataOwner {
    MetadataOwner::new(MetadataLimits {
        main_sockets: bound(1),
        probe_sockets: bound(1),
        probe_attempts_per_initialization: bound(4),
        concurrent_send_calls: bound(1),
        write_pollers: bound(2),
    })
    .unwrap()
}
fn socket() -> MetadataSocketOwner {
    let mut owner = native_owner();
    let grant = owner.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    MetadataSocketOwner::new(owner, grant).unwrap()
}
mod socket;

mod drivers;

mod hosted;

mod callbacks;

mod endpoint;
