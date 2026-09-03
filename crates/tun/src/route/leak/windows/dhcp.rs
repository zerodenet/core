//! Physical interfaces must be able to acquire and renew an address while
//! strict routing is active. These permits are independent of the current
//! gateway/DNS exclusions: a new interface has neither until DHCP completes.

use super::*;
use windows_sys::Win32::NetworkManagement::WindowsFilteringPlatform::{
    FWPM_CONDITION_IP_LOCAL_PORT, FWPM_CONDITION_IP_PROTOCOL, FWPM_CONDITION_IP_REMOTE_PORT,
    FWP_UINT16, FWP_UINT8,
};

const DHCP_PORTS: [(GUID, &str, u16, u16); 2] = [
    (FWPM_LAYER_ALE_AUTH_CONNECT_V4, "dhcp-v4", 68, 67),
    (FWPM_LAYER_ALE_AUTH_CONNECT_V6, "dhcp-v6", 546, 547),
];

pub(super) fn add_permits(
    engine: HANDLE,
    sublayer_key: GUID,
    recovery_key: &str,
    display_name: &str,
) -> io::Result<()> {
    for (layer, scope, local_port, remote_port) in DHCP_PORTS {
        let mut conditions = conditions(local_port, remote_port);
        add_filter(
            engine,
            FilterSpec {
                key: stable_guid(recovery_key, scope),
                layer,
                sublayer_key,
                display_name,
                weight: ALLOW_WEIGHT,
                action: FWP_ACTION_PERMIT,
            },
            &mut conditions,
        )?;
    }
    Ok(())
}

fn conditions(local_port: u16, remote_port: u16) -> [FWPM_FILTER_CONDITION0; 3] {
    [
        FWPM_FILTER_CONDITION0 {
            fieldKey: FWPM_CONDITION_IP_PROTOCOL,
            matchType: FWP_MATCH_EQUAL,
            conditionValue: FWP_CONDITION_VALUE0 {
                r#type: FWP_UINT8,
                Anonymous: FWP_CONDITION_VALUE0_0 { uint8: 17 },
            },
        },
        port_condition(FWPM_CONDITION_IP_LOCAL_PORT, local_port),
        port_condition(FWPM_CONDITION_IP_REMOTE_PORT, remote_port),
    ]
}

fn port_condition(field: GUID, port: u16) -> FWPM_FILTER_CONDITION0 {
    FWPM_FILTER_CONDITION0 {
        fieldKey: field,
        matchType: FWP_MATCH_EQUAL,
        conditionValue: FWP_CONDITION_VALUE0 {
            r#type: FWP_UINT16,
            Anonymous: FWP_CONDITION_VALUE0_0 { uint16: port },
        },
    }
}

#[cfg(test)]
#[path = "tests/dhcp.rs"]
mod tests;
