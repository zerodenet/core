use std::ffi::c_void;
use std::io;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::ptr::{null, null_mut};

use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use serde::{Deserialize, Serialize};
use windows_sys::core::GUID;
use windows_sys::Win32::Foundation::{
    FWP_E_ALREADY_EXISTS, FWP_E_FILTER_NOT_FOUND, FWP_E_SUBLAYER_NOT_FOUND, HANDLE,
};
use windows_sys::Win32::NetworkManagement::IpHelper::ConvertInterfaceAliasToLuid;
use windows_sys::Win32::NetworkManagement::Ndis::NET_LUID_LH;
use windows_sys::Win32::NetworkManagement::WindowsFilteringPlatform::{
    FwpmEngineClose0, FwpmEngineOpen0, FwpmFilterAdd0, FwpmFilterCreateEnumHandle0,
    FwpmFilterDeleteById0, FwpmFilterDestroyEnumHandle0, FwpmFilterEnum0, FwpmFreeMemory0,
    FwpmGetAppIdFromFileName0, FwpmSubLayerAdd0, FwpmSubLayerDeleteByKey0, FwpmTransactionAbort0,
    FwpmTransactionBegin0, FwpmTransactionCommit0, FWPM_ACTION0, FWPM_ACTION0_0,
    FWPM_CONDITION_ALE_APP_ID, FWPM_CONDITION_IP_LOCAL_INTERFACE, FWPM_CONDITION_IP_REMOTE_ADDRESS,
    FWPM_DISPLAY_DATA0, FWPM_FILTER0, FWPM_FILTER0_0, FWPM_FILTER_CONDITION0,
    FWPM_FILTER_ENUM_TEMPLATE0, FWPM_FILTER_FLAG_PERSISTENT, FWPM_LAYER_ALE_AUTH_CONNECT_V4,
    FWPM_LAYER_ALE_AUTH_CONNECT_V6, FWPM_SUBLAYER0, FWPM_SUBLAYER_FLAG_PERSISTENT,
    FWP_ACTION_BLOCK, FWP_ACTION_PERMIT, FWP_BYTE_BLOB, FWP_BYTE_BLOB_TYPE, FWP_CONDITION_VALUE0,
    FWP_CONDITION_VALUE0_0, FWP_EMPTY, FWP_FILTER_ENUM_FULLY_CONTAINED, FWP_MATCH_EQUAL,
    FWP_UINT64, FWP_V4_ADDR_AND_MASK, FWP_V4_ADDR_MASK, FWP_V6_ADDR_AND_MASK, FWP_V6_ADDR_MASK,
    FWP_VALUE0, FWP_VALUE0_0,
};
use windows_sys::Win32::System::Rpc::RPC_C_AUTHN_WINNT;

use super::{
    normalized_exclusions, normalized_prefixes, safe_resource_name, validate_interface_name,
};
use crate::route::journal::route_state_root;

mod dhcp;

const ALLOW_WEIGHT: u64 = u64::MAX;
const BLOCK_WEIGHT: u64 = 1;
const SUBLAYER_WEIGHT: u16 = 0x7fff;

pub struct SystemLeakGuard {
    sublayer_key: GUID,
    recovery_key: String,
    display_name: String,
    tun_name: String,
    protected: Vec<IpNet>,
    excluded: Vec<IpAddr>,
    active: bool,
}

impl std::fmt::Debug for SystemLeakGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SystemLeakGuard")
            .field("recovery_key", &self.recovery_key)
            .field("display_name", &self.display_name)
            .field("tun_name", &self.tun_name)
            .field("protected", &self.protected)
            .field("excluded", &self.excluded)
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyFirewallJournal {
    schema: String,
    profiles: Vec<LegacyProfilePolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyProfilePolicy {
    name: String,
    action: String,
}

impl SystemLeakGuard {
    pub fn install(
        tun_name: &str,
        recovery_key: &str,
        protected: &[IpNet],
        excluded: &[IpAddr],
    ) -> io::Result<Self> {
        validate_interface_name(tun_name)?;
        migrate_legacy_policy(recovery_key)?;

        let safe_name = safe_resource_name(recovery_key);
        let display_name = format!("Zero strict route ({safe_name})");
        let sublayer_key = stable_guid(recovery_key, "sublayer");
        let protected = normalized_prefixes(protected);
        let excluded = normalized_exclusions(excluded);
        install_policy(
            sublayer_key,
            recovery_key,
            &display_name,
            tun_name,
            &protected,
            &excluded,
        )?;

        Ok(Self {
            sublayer_key,
            recovery_key: recovery_key.to_owned(),
            display_name,
            tun_name: tun_name.to_owned(),
            protected,
            excluded,
            active: true,
        })
    }

    pub fn reconcile(&mut self, protected: &[IpNet], excluded: &[IpAddr]) -> io::Result<bool> {
        let protected = normalized_prefixes(protected);
        let excluded = normalized_exclusions(excluded);
        let policy_changed = protected != self.protected || excluded != self.excluded;
        let expected = expected_filter_keys(&self.recovery_key, &protected, &excluded);
        let complete = policy_is_complete(self.sublayer_key, &expected)?;
        if !policy_changed && complete {
            return Ok(false);
        }

        install_policy(
            self.sublayer_key,
            &self.recovery_key,
            &self.display_name,
            &self.tun_name,
            &protected,
            &excluded,
        )?;
        self.protected = protected;
        self.excluded = excluded;
        Ok(true)
    }

    pub fn close(mut self) -> io::Result<()> {
        self.cleanup()
    }

    fn cleanup(&mut self) -> io::Result<()> {
        if !self.active {
            return Ok(());
        }
        remove_policy(self.sublayer_key)?;
        self.active = false;
        Ok(())
    }
}

impl Drop for SystemLeakGuard {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

fn install_policy(
    sublayer_key: GUID,
    recovery_key: &str,
    display_name: &str,
    tun_name: &str,
    protected: &[IpNet],
    excluded: &[IpAddr],
) -> io::Result<()> {
    let engine = WfpEngine::open()?;
    let existing = owned_filter_inventory(engine.handle, sublayer_key)?;
    let app_id = AppId::current()?;
    let mut tun_luid = interface_luid(tun_name)?;
    let mut transaction = WfpTransaction::begin(&engine)?;

    ensure_sublayer(engine.handle, sublayer_key, display_name)?;
    delete_filter_ids(engine.handle, &existing.ids)?;
    add_app_permits(
        engine.handle,
        sublayer_key,
        recovery_key,
        display_name,
        &app_id,
    )?;
    add_interface_permits(
        engine.handle,
        sublayer_key,
        recovery_key,
        display_name,
        &mut tun_luid,
    )?;
    dhcp::add_permits(engine.handle, sublayer_key, recovery_key, display_name)?;
    add_remote_filter(
        engine.handle,
        sublayer_key,
        recovery_key,
        display_name,
        &IpNet::V4(Ipv4Net::new(Ipv4Addr::LOCALHOST, 8).expect("valid IPv4 loopback")),
        "loopback-v4",
        ALLOW_WEIGHT,
        FWP_ACTION_PERMIT,
    )?;
    add_remote_filter(
        engine.handle,
        sublayer_key,
        recovery_key,
        display_name,
        &IpNet::V6(Ipv6Net::new(Ipv6Addr::LOCALHOST, 128).expect("valid IPv6 loopback")),
        "loopback-v6",
        ALLOW_WEIGHT,
        FWP_ACTION_PERMIT,
    )?;
    for address in excluded {
        let prefix = exact_prefix(*address);
        add_remote_filter(
            engine.handle,
            sublayer_key,
            recovery_key,
            display_name,
            &prefix,
            &format!("exclude:{prefix}"),
            ALLOW_WEIGHT,
            FWP_ACTION_PERMIT,
        )?;
    }
    for prefix in protected {
        add_remote_filter(
            engine.handle,
            sublayer_key,
            recovery_key,
            display_name,
            prefix,
            &format!("block:{prefix}"),
            BLOCK_WEIGHT,
            FWP_ACTION_BLOCK,
        )?;
    }

    transaction.commit()
}

fn remove_policy(sublayer_key: GUID) -> io::Result<()> {
    let engine = WfpEngine::open()?;
    let existing = owned_filter_inventory(engine.handle, sublayer_key)?;
    let mut transaction = WfpTransaction::begin(&engine)?;
    delete_filter_ids(engine.handle, &existing.ids)?;
    let status = unsafe { FwpmSubLayerDeleteByKey0(engine.handle, &sublayer_key) };
    if status != 0 && status != FWP_E_SUBLAYER_NOT_FOUND as u32 {
        return Err(wfp_error("delete strict-route WFP sublayer", status));
    }
    transaction.commit()
}

fn policy_is_complete(sublayer_key: GUID, expected: &[GUID]) -> io::Result<bool> {
    let engine = WfpEngine::open()?;
    let inventory = owned_filter_inventory(engine.handle, sublayer_key)?;
    Ok(inventory.keys.len() == expected.len()
        && expected.iter().all(|expected| {
            inventory
                .keys
                .iter()
                .any(|actual| guid_eq(actual, expected))
        }))
}

fn ensure_sublayer(engine: HANDLE, sublayer_key: GUID, display_name: &str) -> io::Result<()> {
    let mut name = wide(display_name);
    let sublayer = FWPM_SUBLAYER0 {
        subLayerKey: sublayer_key,
        displayData: FWPM_DISPLAY_DATA0 {
            name: name.as_mut_ptr(),
            description: null_mut(),
        },
        flags: FWPM_SUBLAYER_FLAG_PERSISTENT,
        providerKey: null_mut(),
        providerData: empty_blob(),
        weight: SUBLAYER_WEIGHT,
    };
    let status = unsafe { FwpmSubLayerAdd0(engine, &sublayer, null_mut()) };
    if status == 0 || status == FWP_E_ALREADY_EXISTS as u32 {
        Ok(())
    } else {
        Err(wfp_error("create strict-route WFP sublayer", status))
    }
}

fn add_app_permits(
    engine: HANDLE,
    sublayer_key: GUID,
    recovery_key: &str,
    display_name: &str,
    app_id: &AppId,
) -> io::Result<()> {
    for (layer, scope) in [
        (FWPM_LAYER_ALE_AUTH_CONNECT_V4, "app-v4"),
        (FWPM_LAYER_ALE_AUTH_CONNECT_V6, "app-v6"),
    ] {
        let mut condition = FWPM_FILTER_CONDITION0 {
            fieldKey: FWPM_CONDITION_ALE_APP_ID,
            matchType: FWP_MATCH_EQUAL,
            conditionValue: FWP_CONDITION_VALUE0 {
                r#type: FWP_BYTE_BLOB_TYPE,
                Anonymous: FWP_CONDITION_VALUE0_0 {
                    byteBlob: app_id.as_ptr(),
                },
            },
        };
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
            std::slice::from_mut(&mut condition),
        )?;
    }
    Ok(())
}

fn add_interface_permits(
    engine: HANDLE,
    sublayer_key: GUID,
    recovery_key: &str,
    display_name: &str,
    tun_luid: &mut u64,
) -> io::Result<()> {
    for (layer, scope) in [
        (FWPM_LAYER_ALE_AUTH_CONNECT_V4, "tun-v4"),
        (FWPM_LAYER_ALE_AUTH_CONNECT_V6, "tun-v6"),
    ] {
        let mut condition = FWPM_FILTER_CONDITION0 {
            fieldKey: FWPM_CONDITION_IP_LOCAL_INTERFACE,
            matchType: FWP_MATCH_EQUAL,
            conditionValue: FWP_CONDITION_VALUE0 {
                r#type: FWP_UINT64,
                Anonymous: FWP_CONDITION_VALUE0_0 {
                    uint64: std::ptr::from_mut(&mut *tun_luid),
                },
            },
        };
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
            std::slice::from_mut(&mut condition),
        )?;
    }
    Ok(())
}

fn add_remote_filter(
    engine: HANDLE,
    sublayer_key: GUID,
    recovery_key: &str,
    display_name: &str,
    prefix: &IpNet,
    scope: &str,
    weight: u64,
    action: u32,
) -> io::Result<()> {
    let spec = FilterSpec {
        key: stable_guid(recovery_key, scope),
        layer: layer_for_prefix(prefix),
        sublayer_key,
        display_name,
        weight,
        action,
    };
    match prefix {
        IpNet::V4(prefix) => {
            let mut mask = v4_address_mask(prefix);
            let mut condition = FWPM_FILTER_CONDITION0 {
                fieldKey: FWPM_CONDITION_IP_REMOTE_ADDRESS,
                matchType: FWP_MATCH_EQUAL,
                conditionValue: FWP_CONDITION_VALUE0 {
                    r#type: FWP_V4_ADDR_MASK,
                    Anonymous: FWP_CONDITION_VALUE0_0 {
                        v4AddrMask: &mut mask,
                    },
                },
            };
            add_filter(engine, spec, std::slice::from_mut(&mut condition))
        }
        IpNet::V6(prefix) => {
            let mut mask = FWP_V6_ADDR_AND_MASK {
                addr: prefix.network().octets(),
                prefixLength: prefix.prefix_len(),
            };
            let mut condition = FWPM_FILTER_CONDITION0 {
                fieldKey: FWPM_CONDITION_IP_REMOTE_ADDRESS,
                matchType: FWP_MATCH_EQUAL,
                conditionValue: FWP_CONDITION_VALUE0 {
                    r#type: FWP_V6_ADDR_MASK,
                    Anonymous: FWP_CONDITION_VALUE0_0 {
                        v6AddrMask: &mut mask,
                    },
                },
            };
            add_filter(engine, spec, std::slice::from_mut(&mut condition))
        }
    }
}

struct FilterSpec<'a> {
    key: GUID,
    layer: GUID,
    sublayer_key: GUID,
    display_name: &'a str,
    weight: u64,
    action: u32,
}

fn add_filter(
    engine: HANDLE,
    spec: FilterSpec<'_>,
    conditions: &mut [FWPM_FILTER_CONDITION0],
) -> io::Result<()> {
    let mut display_name = wide(spec.display_name);
    let mut weight = spec.weight;
    let filter = FWPM_FILTER0 {
        filterKey: spec.key,
        displayData: FWPM_DISPLAY_DATA0 {
            name: display_name.as_mut_ptr(),
            description: null_mut(),
        },
        flags: FWPM_FILTER_FLAG_PERSISTENT,
        providerKey: null_mut(),
        providerData: empty_blob(),
        layerKey: spec.layer,
        subLayerKey: spec.sublayer_key,
        weight: FWP_VALUE0 {
            r#type: FWP_UINT64,
            Anonymous: FWP_VALUE0_0 {
                uint64: &mut weight,
            },
        },
        numFilterConditions: conditions.len() as u32,
        filterCondition: conditions.as_mut_ptr(),
        action: FWPM_ACTION0 {
            r#type: spec.action,
            Anonymous: FWPM_ACTION0_0 {
                filterType: GUID::from_u128(0),
            },
        },
        Anonymous: FWPM_FILTER0_0 { rawContext: 0 },
        reserved: null_mut(),
        filterId: 0,
        effectiveWeight: FWP_VALUE0 {
            r#type: FWP_EMPTY,
            Anonymous: FWP_VALUE0_0 { uint64: null_mut() },
        },
    };
    let mut id = 0_u64;
    let status = unsafe { FwpmFilterAdd0(engine, &filter, null_mut(), &mut id) };
    if status == 0 {
        Ok(())
    } else {
        Err(wfp_error("add strict-route WFP filter", status))
    }
}

#[derive(Default)]
struct FilterInventory {
    ids: Vec<u64>,
    keys: Vec<GUID>,
}

fn owned_filter_inventory(engine: HANDLE, sublayer_key: GUID) -> io::Result<FilterInventory> {
    let mut inventory = FilterInventory::default();
    for layer in [
        FWPM_LAYER_ALE_AUTH_CONNECT_V4,
        FWPM_LAYER_ALE_AUTH_CONNECT_V6,
    ] {
        let mut template = FWPM_FILTER_ENUM_TEMPLATE0 {
            providerKey: null_mut(),
            layerKey: layer,
            enumType: FWP_FILTER_ENUM_FULLY_CONTAINED,
            flags: 0,
            providerContextTemplate: null_mut(),
            numFilterConditions: 0,
            filterCondition: null_mut(),
            actionMask: u32::MAX,
            calloutKey: null_mut(),
        };
        let mut enum_handle = null_mut();
        let status =
            unsafe { FwpmFilterCreateEnumHandle0(engine, &mut template, &mut enum_handle) };
        if status != 0 {
            return Err(wfp_error("enumerate strict-route WFP filters", status));
        }
        let enumeration = FilterEnumeration {
            engine,
            handle: enum_handle,
        };
        loop {
            let mut entries: *mut *mut FWPM_FILTER0 = null_mut();
            let mut count = 0_u32;
            let status = unsafe {
                FwpmFilterEnum0(engine, enumeration.handle, 256, &mut entries, &mut count)
            };
            if status != 0 {
                free_wfp_memory(entries.cast());
                return Err(wfp_error("read strict-route WFP filters", status));
            }
            if count != 0 && entries.is_null() {
                return Err(io::Error::other(
                    "read strict-route WFP filters: Windows returned a null filter list",
                ));
            }
            for index in 0..count as usize {
                let filter = unsafe { *entries.add(index) };
                if filter.is_null() {
                    continue;
                }
                let filter = unsafe { &*filter };
                if guid_eq(&filter.subLayerKey, &sublayer_key) {
                    inventory.ids.push(filter.filterId);
                    inventory.keys.push(filter.filterKey);
                }
            }
            free_wfp_memory(entries.cast());
            if count < 256 {
                break;
            }
        }
    }
    Ok(inventory)
}

fn delete_filter_ids(engine: HANDLE, ids: &[u64]) -> io::Result<()> {
    for id in ids {
        let status = unsafe { FwpmFilterDeleteById0(engine, *id) };
        if status != 0 && status != FWP_E_FILTER_NOT_FOUND as u32 {
            return Err(wfp_error("delete stale strict-route WFP filter", status));
        }
    }
    Ok(())
}

fn expected_filter_keys(recovery_key: &str, protected: &[IpNet], excluded: &[IpAddr]) -> Vec<GUID> {
    let mut keys = [
        "app-v4",
        "app-v6",
        "tun-v4",
        "tun-v6",
        "loopback-v4",
        "loopback-v6",
        "dhcp-v4",
        "dhcp-v6",
    ]
    .into_iter()
    .map(|scope| stable_guid(recovery_key, scope))
    .collect::<Vec<_>>();
    keys.extend(excluded.iter().map(|address| {
        let prefix = exact_prefix(*address);
        stable_guid(recovery_key, &format!("exclude:{prefix}"))
    }));
    keys.extend(
        protected
            .iter()
            .map(|prefix| stable_guid(recovery_key, &format!("block:{prefix}"))),
    );
    keys
}

fn exact_prefix(address: IpAddr) -> IpNet {
    match address {
        IpAddr::V4(address) => {
            IpNet::V4(Ipv4Net::new(address, 32).expect("valid IPv4 host prefix"))
        }
        IpAddr::V6(address) => {
            IpNet::V6(Ipv6Net::new(address, 128).expect("valid IPv6 host prefix"))
        }
    }
}

fn layer_for_prefix(prefix: &IpNet) -> GUID {
    match prefix {
        IpNet::V4(_) => FWPM_LAYER_ALE_AUTH_CONNECT_V4,
        IpNet::V6(_) => FWPM_LAYER_ALE_AUTH_CONNECT_V6,
    }
}

fn v4_address_mask(prefix: &Ipv4Net) -> FWP_V4_ADDR_AND_MASK {
    let prefix_len = prefix.prefix_len();
    FWP_V4_ADDR_AND_MASK {
        addr: u32::from_be_bytes(prefix.network().octets()),
        mask: if prefix_len == 0 {
            0
        } else {
            u32::MAX << (32 - prefix_len)
        },
    }
}

fn interface_luid(tun_name: &str) -> io::Result<u64> {
    let alias = wide(tun_name);
    let mut luid = NET_LUID_LH { Value: 0 };
    let status = unsafe { ConvertInterfaceAliasToLuid(alias.as_ptr(), &mut luid) };
    if status == 0 {
        Ok(unsafe { luid.Value })
    } else {
        Err(io::Error::other(format!(
            "resolve Wintun interface `{tun_name}` LUID: Windows error {status}"
        )))
    }
}

struct AppId(*mut FWP_BYTE_BLOB);

impl AppId {
    fn current() -> io::Result<Self> {
        let executable = std::env::current_exe()?;
        let path = executable
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let mut app_id = null_mut();
        let status = unsafe { FwpmGetAppIdFromFileName0(path.as_ptr(), &mut app_id) };
        if status != 0 {
            return Err(wfp_error("resolve Zero executable WFP AppID", status));
        }
        if app_id.is_null() {
            return Err(io::Error::other(
                "resolve Zero executable WFP AppID: Windows returned no identifier",
            ));
        }
        Ok(Self(app_id))
    }

    fn as_ptr(&self) -> *mut FWP_BYTE_BLOB {
        self.0
    }
}

impl Drop for AppId {
    fn drop(&mut self) {
        free_wfp_memory(self.0.cast());
        self.0 = null_mut();
    }
}

struct WfpEngine {
    handle: HANDLE,
}

impl WfpEngine {
    fn open() -> io::Result<Self> {
        let mut handle = null_mut();
        let status =
            unsafe { FwpmEngineOpen0(null(), RPC_C_AUTHN_WINNT, null(), null(), &mut handle) };
        if status == 0 {
            Ok(Self { handle })
        } else {
            Err(wfp_error("open Windows Filtering Platform engine", status))
        }
    }
}

impl Drop for WfpEngine {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { FwpmEngineClose0(self.handle) };
            self.handle = null_mut();
        }
    }
}

struct WfpTransaction<'a> {
    engine: &'a WfpEngine,
    active: bool,
}

impl<'a> WfpTransaction<'a> {
    fn begin(engine: &'a WfpEngine) -> io::Result<Self> {
        let status = unsafe { FwpmTransactionBegin0(engine.handle, 0) };
        if status == 0 {
            Ok(Self {
                engine,
                active: true,
            })
        } else {
            Err(wfp_error("begin strict-route WFP transaction", status))
        }
    }

    fn commit(&mut self) -> io::Result<()> {
        let status = unsafe { FwpmTransactionCommit0(self.engine.handle) };
        if status == 0 {
            self.active = false;
            Ok(())
        } else {
            Err(wfp_error("commit strict-route WFP transaction", status))
        }
    }
}

impl Drop for WfpTransaction<'_> {
    fn drop(&mut self) {
        if self.active {
            unsafe { FwpmTransactionAbort0(self.engine.handle) };
        }
    }
}

struct FilterEnumeration {
    engine: HANDLE,
    handle: HANDLE,
}

impl Drop for FilterEnumeration {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { FwpmFilterDestroyEnumHandle0(self.engine, self.handle) };
        }
    }
}

fn empty_blob() -> FWP_BYTE_BLOB {
    FWP_BYTE_BLOB {
        size: 0,
        data: null_mut(),
    }
}

fn free_wfp_memory(pointer: *mut c_void) {
    if pointer.is_null() {
        return;
    }
    let mut pointer = pointer;
    unsafe { FwpmFreeMemory0(&mut pointer) };
}

fn wfp_error(action: &str, status: u32) -> io::Error {
    io::Error::other(format!("{action}: WFP status 0x{status:08x}"))
}

fn wide(value: &str) -> Vec<u16> {
    std::ffi::OsStr::new(value)
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}

fn stable_guid(recovery_key: &str, scope: &str) -> GUID {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    fn hash(seed: u64, parts: &[&[u8]]) -> u64 {
        parts.iter().fold(seed, |hash, part| {
            let hash = part.iter().fold(hash, |hash, byte| {
                (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
            });
            (hash ^ 0xff).wrapping_mul(FNV_PRIME)
        })
    }

    let parts = [
        b"zero.strict-route.wfp".as_slice(),
        recovery_key.as_bytes(),
        scope.as_bytes(),
    ];
    let high = hash(FNV_OFFSET, &parts);
    let low = hash(FNV_OFFSET ^ 0x9e37_79b9_7f4a_7c15, &parts);
    let mut bytes = (u128::from(high) << 64 | u128::from(low)).to_be_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    GUID::from_u128(u128::from_be_bytes(bytes))
}

fn guid_eq(left: &GUID, right: &GUID) -> bool {
    left.data1 == right.data1
        && left.data2 == right.data2
        && left.data3 == right.data3
        && left.data4 == right.data4
}

fn migrate_legacy_policy(recovery_key: &str) -> io::Result<()> {
    let safe_name = safe_resource_name(recovery_key);
    let journal_path = route_state_root()?.join(format!("leak-{safe_name}.json"));
    let Some(journal) = read_legacy_journal(&journal_path)? else {
        return Ok(());
    };
    restore_legacy_profiles(&format!("ZeroKillSwitch-{safe_name}"), &journal)?;
    match std::fs::remove_file(journal_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn read_legacy_journal(path: &Path) -> io::Result<Option<LegacyFirewallJournal>> {
    let raw = match std::fs::read(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let journal = parse_legacy_profile_snapshot(&raw)?;
    Ok(Some(journal))
}

fn parse_legacy_profile_snapshot(output: &[u8]) -> io::Result<LegacyFirewallJournal> {
    if output.iter().all(u8::is_ascii_whitespace) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Windows Firewall profile snapshot produced empty output",
        ));
    }
    let journal = serde_json::from_slice::<LegacyFirewallJournal>(output).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("parse Windows Firewall profile snapshot: {error}"),
        )
    })?;
    validate_legacy_journal(&journal)?;
    Ok(journal)
}

fn validate_legacy_journal(journal: &LegacyFirewallJournal) -> io::Result<()> {
    const PROFILE_NAMES: [&str; 3] = ["Domain", "Private", "Public"];
    let complete = journal.schema == "zero.tun.leak-guard.v1"
        && journal.profiles.len() == PROFILE_NAMES.len()
        && PROFILE_NAMES.iter().all(|name| {
            journal
                .profiles
                .iter()
                .filter(|profile| profile.name.as_str() == *name)
                .count()
                == 1
        })
        && journal
            .profiles
            .iter()
            .all(|profile| matches!(profile.action.as_str(), "Allow" | "Block" | "NotConfigured"));
    if complete {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Windows Firewall profile snapshot is incomplete",
        ))
    }
}

fn restore_legacy_profiles(group: &str, journal: &LegacyFirewallJournal) -> io::Result<()> {
    let mut script = "$ErrorActionPreference='Stop'; ".to_owned();
    for profile in &journal.profiles {
        script.push_str(&format!(
            "Set-NetFirewallProfile -Name '{}' -DefaultOutboundAction {}; ",
            quote_powershell(&profile.name),
            profile.action
        ));
    }
    script.push_str(&format!(
        "Get-NetFirewallRule -Group '{}' -ErrorAction SilentlyContinue | Remove-NetFirewallRule",
        quote_powershell(group)
    ));
    run_powershell(&script).map(|_| ())
}

fn run_powershell(script: &str) -> io::Result<Vec<u8>> {
    let mut child = Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "PowerShell stdin unavailable"))?
        .write_all(script.as_bytes())?;
    let output = child.wait_with_output()?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "restore legacy Windows Firewall kill switch: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        ))
    }
}

fn quote_powershell(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_guids_are_instance_and_scope_specific() {
        let key = stable_guid("tun-in", "sublayer");
        assert!(guid_eq(&key, &stable_guid("tun-in", "sublayer")));
        assert!(!guid_eq(&key, &stable_guid("other", "sublayer")));
        assert!(!guid_eq(&key, &stable_guid("tun-in", "app-v4")));
    }

    #[test]
    fn ipv4_networks_are_encoded_as_wfp_address_masks() {
        let mask = v4_address_mask(&"192.0.2.0/24".parse().unwrap());
        assert_eq!(mask.addr, 0xc000_0200);
        assert_eq!(mask.mask, 0xffff_ff00);

        let default = v4_address_mask(&"0.0.0.0/0".parse().unwrap());
        assert_eq!(default.mask, 0);
    }

    #[test]
    fn legacy_profile_snapshot_requires_explicit_complete_json() {
        let error = parse_legacy_profile_snapshot(b"").expect_err("empty output must fail closed");
        assert!(error.to_string().contains("empty output"));

        let journal = parse_legacy_profile_snapshot(
            br#"{"schema":"zero.tun.leak-guard.v1","profiles":[{"name":"Domain","action":"Allow"},{"name":"Private","action":"Block"},{"name":"Public","action":"NotConfigured"}]}"#,
        )
        .expect("complete profile snapshot");
        assert_eq!(journal.profiles.len(), 3);

        let error = parse_legacy_profile_snapshot(
            br#"{"schema":"zero.tun.leak-guard.v1","profiles":[{"name":"Domain","action":"Allow"}]}"#,
        )
        .expect_err("partial snapshot must fail closed");
        assert!(error.to_string().contains("incomplete"));
    }

    #[test]
    fn powershell_values_are_single_quote_escaped_for_legacy_migration() {
        assert_eq!(quote_powershell("a'b"), "a''b");
    }
}
