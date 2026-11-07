//! The same TunInfo/egress owners; strings, IP networks and errors stay borrowed.
use super::*;
use crate::runtime::TunInfo;
use serde::ser::{SerializeMap, SerializeSeq};
use std::sync::{atomic::Ordering, MutexGuard};
pub(super) struct Tun<'a> {
    // Inner egress/read observations drop before their outer Tun owner mutex.
    egress: zero_platform_tokio::EgressReadObservation<'a>,
    last: Option<MutexGuard<'a, Option<String>>>,
    info: MutexGuard<'a, Option<TunInfo>>,
    fake_ip: bool,
    dns_queries: u64,
    policy: &'static str,
}
impl<'a> Tun<'a> {
    pub(super) fn new(handle: &'a ProxyHandle) -> Self {
        let info = handle
            .proxy
            .tun_info
            .lock()
            .expect("TUN status lock poisoned");
        let last = info.is_none().then(|| {
            handle
                .proxy
                .tun_last_error
                .lock()
                .expect("TUN last error lock poisoned")
        });
        // Never take DNS inner after holding egress: reload's original route update
        // may hold DNS state before changing egress. These are only scalar reads.
        let (policy, fake_ip) = handle.proxy.resolver.address_family_and_fake_ip();
        let dns_queries = info
            .as_ref()
            .map_or(0, |tun| tun.dns_hijacked_queries.load(Ordering::Relaxed));
        let fake_ip = info.is_some() && fake_ip; // Preserve old absent-TUN default exactly.
        let egress = handle.proxy.egress_interface.read_observation();
        Self {
            egress,
            last,
            info,
            fake_ip,
            dns_queries,
            policy: policy.as_str(),
        }
    }
}
struct Networks<'a>(&'a [ipnet::IpNet]);
impl Serialize for Networks<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_seq(Some(self.0.len()))?;
        for network in self.0 {
            out.serialize_element(&Displayed(network))?;
        }
        out.end()
    }
}
struct Displayed<'a>(&'a dyn std::fmt::Display);
impl Serialize for Displayed<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self.0)
    }
}
struct Family<'a> {
    egress: &'a zero_platform_tokio::EgressReadObservation<'a>,
    ipv6: bool,
}
impl Serialize for Family<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use zero_api::TunFamilyEgressAvailability as Availability;
        let interface = self.egress.interface(self.ipv6);
        let reason = self.egress.unavailable_reason(self.ipv6);
        let (availability, interface, reason) = match (interface, reason) {
            (Some(interface), _) => (Availability::Available, Some(interface.name()), None),
            (None, Some(reason)) => (Availability::Unavailable, None, Some(reason)),
            (None, None) => (Availability::Unknown, None, None),
        };
        let mut out = s.serialize_map(None)?;
        out.serialize_entry("availability", &availability)?;
        if let Some(interface) = interface {
            out.serialize_entry("interface", interface)?;
        }
        if let Some(reason) = reason {
            out.serialize_entry("reason", reason)?;
        }
        out.end()
    }
}
impl Serialize for Tun<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_map(None)?;
        let tun = self.info.as_ref();
        out.serialize_entry("running", &tun.is_some())?;
        if let Some(tun) = tun {
            out.serialize_entry("name", &tun.name)?;
            out.serialize_entry("addr", &tun.addr)?;
            if !tun.addresses.is_empty() {
                out.serialize_entry("addresses", &tun.addresses)?;
            }
            out.serialize_entry("mtu", &tun.mtu)?;
            out.serialize_entry("tag", &tun.tag)?;
            out.serialize_entry("healthy", &tun.healthy)?;
            out.serialize_entry("auto_route", &tun.auto_route)?;
            if !tun.include_cidrs.is_empty() {
                out.serialize_entry("include_cidrs", &Networks(&tun.include_cidrs))?;
            }
            if !tun.exclude_cidrs.is_empty() {
                out.serialize_entry("exclude_cidrs", &Networks(&tun.exclude_cidrs))?;
            }
            out.serialize_entry("dual_stack", &tun.dual_stack)?;
            out.serialize_entry("strict_route", &tun.strict_route)?;
            out.serialize_entry("dns_hijack", &tun.dns_hijack)?;
        } else {
            out.serialize_entry("healthy", &false)?;
            out.serialize_entry("auto_route", &false)?;
            out.serialize_entry("dual_stack", &false)?;
            out.serialize_entry("strict_route", &false)?;
            out.serialize_entry("dns_hijack", &false)?;
        }
        out.serialize_entry("fake_ip_enabled", &self.fake_ip)?;
        out.serialize_entry("dns_hijacked_queries", &self.dns_queries)?;
        if let Some(tun) = tun {
            if let Some(name) = &tun.egress_interface {
                out.serialize_entry("egress_interface", name)?;
            }
            if let Some(name) = &tun.egress_interface_v4 {
                out.serialize_entry("egress_interface_v4", name)?;
            }
            if let Some(name) = &tun.egress_interface_v6 {
                out.serialize_entry("egress_interface_v6", name)?;
            }
        }
        out.serialize_entry(
            "ipv4_egress",
            &Family {
                egress: &self.egress,
                ipv6: false,
            },
        )?;
        out.serialize_entry(
            "ipv6_egress",
            &Family {
                egress: &self.egress,
                ipv6: true,
            },
        )?;
        out.serialize_entry("network_generation", &self.egress.generation())?;
        out.serialize_entry("address_family_policy", self.policy)?;
        out.serialize_entry(
            "ipv6_to_ipv4_fallbacks",
            &self.egress.ipv6_to_ipv4_fallbacks(),
        )?;
        let last = tun
            .and_then(|tun| tun.last_error.as_ref())
            .or_else(|| self.last.as_ref().and_then(|last| last.as_ref()));
        if let Some(last) = last {
            out.serialize_entry("last_error", last)?;
        }
        out.serialize_entry(
            "managed_by_config",
            &tun.is_some_and(|tun| tun.managed_config.is_some()),
        )?;
        out.end()
    }
}
