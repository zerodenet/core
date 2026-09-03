use super::*;

// Evaluate the actual WFP condition array, including field keys and value
// types, so reversed ports or an accidentally removed constraint fail tests.
fn permits(conditions: &[FWPM_FILTER_CONDITION0], protocol: u8, local: u16, remote: u16) -> bool {
    conditions.iter().all(|condition| {
        assert_eq!(condition.matchType, FWP_MATCH_EQUAL);
        if guid_eq(&condition.fieldKey, &FWPM_CONDITION_IP_PROTOCOL) {
            assert_eq!(condition.conditionValue.r#type, FWP_UINT8);
            unsafe { condition.conditionValue.Anonymous.uint8 == protocol }
        } else {
            assert_eq!(condition.conditionValue.r#type, FWP_UINT16);
            let port = unsafe { condition.conditionValue.Anonymous.uint16 };
            if guid_eq(&condition.fieldKey, &FWPM_CONDITION_IP_LOCAL_PORT) {
                port == local
            } else {
                assert!(guid_eq(&condition.fieldKey, &FWPM_CONDITION_IP_REMOTE_PORT));
                port == remote
            }
        }
    })
}

#[test]
fn dhcp_permits_only_client_udp_port_pairs() {
    for (_, _, local, remote) in DHCP_PORTS {
        let rules = conditions(local, remote);
        assert!(permits(&rules, 17, local, remote));
        assert!(!permits(&rules, 6, local, remote), "TCP must stay blocked");
        assert!(!permits(&rules, 17, local + 1, remote));
        assert!(!permits(&rules, 17, local, remote + 1));
        assert!(
            !permits(&rules, 17, remote, local),
            "server traffic is not a client exception"
        );
        assert!(
            !permits(&rules, 17, 8235, 8235),
            "ordinary LAN broadcasts stay blocked"
        );
        assert!(
            !permits(&rules, 17, local, 53),
            "DNS is not a DHCP exception"
        );
    }
}

#[test]
fn dhcp_permits_cover_both_families_before_an_egress_exists() {
    assert!(guid_eq(&DHCP_PORTS[0].0, &FWPM_LAYER_ALE_AUTH_CONNECT_V4));
    assert!(guid_eq(&DHCP_PORTS[1].0, &FWPM_LAYER_ALE_AUTH_CONNECT_V6));
    let keys = expected_filter_keys("tun", &[], &[]);
    for (_, scope, _, _) in DHCP_PORTS {
        let key = stable_guid("tun", scope);
        assert!(keys.iter().any(|candidate| guid_eq(candidate, &key)));
    }
    assert_eq!(
        keys.len(),
        8,
        "reconciliation must detect old policies without DHCP permits"
    );
}
