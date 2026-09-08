use super::is_topology_change;

fn message(kind: i32) -> Vec<u8> {
    let length = std::mem::size_of::<libc::rt_msghdr>();
    let mut message = vec![0; length];
    message[..2].copy_from_slice(&(length as u16).to_ne_bytes());
    message[2] = libc::RTM_VERSION as u8;
    message[3] = kind as u8;
    message
}

#[test]
fn queries_do_not_feed_back_into_reconciliation() {
    for kind in [
        libc::RTM_GET,
        libc::RTM_GET2,
        libc::RTM_MISS,
        libc::RTM_RESOLVE,
    ] {
        assert!(!is_topology_change(&message(kind)));
    }
}

#[test]
fn link_address_and_route_changes_trigger_reconciliation() {
    for kind in [
        libc::RTM_ADD,
        libc::RTM_DELETE,
        libc::RTM_CHANGE,
        libc::RTM_NEWADDR,
        libc::RTM_DELADDR,
        libc::RTM_IFINFO,
        libc::RTM_IFINFO2,
    ] {
        assert!(is_topology_change(&message(kind)));
    }
    assert!(!is_topology_change(&[0, 0, 0]));
    assert!(!is_topology_change(&[
        255,
        255,
        libc::RTM_VERSION as u8,
        libc::RTM_ADD as u8
    ]));
}

#[test]
fn neighbor_cache_churn_and_failed_route_commands_do_not_invalidate_network() {
    for flags in [libc::RTF_LLINFO, libc::RTF_WASCLONED] {
        let mut event = message(libc::RTM_ADD);
        let offset = std::mem::offset_of!(libc::rt_msghdr, rtm_flags);
        event[offset..offset + 4].copy_from_slice(&flags.to_ne_bytes());
        assert!(!is_topology_change(&event));
    }
    let mut event = message(libc::RTM_ADD);
    let offset = std::mem::offset_of!(libc::rt_msghdr, rtm_errno);
    event[offset..offset + 4].copy_from_slice(&libc::EEXIST.to_ne_bytes());
    assert!(!is_topology_change(&event));
}
