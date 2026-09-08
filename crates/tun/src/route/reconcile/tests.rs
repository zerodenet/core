use std::net::{IpAddr, Ipv4Addr};

use super::*;

struct FakeRouteState {
    egress: RouteInterface,
    gateway: String,
    excluded: Vec<IpAddr>,
    owned: Vec<IpAddr>,
    fail_next_install: bool,
    fail_next_reconcile: bool,
    route_missing: bool,
    audit_fails: bool,
    audits: usize,
}

impl RouteReconcileState for FakeRouteState {
    type Gateway = String;

    fn repair_routes(&mut self) -> io::Result<bool> {
        self.audits += 1;
        if self.audit_fails {
            return Err(io::Error::other("route verification failed"));
        }
        Ok(std::mem::take(&mut self.route_missing))
    }

    fn current_egress(&self) -> &RouteInterface {
        &self.egress
    }

    fn current_gateway(&self) -> &Self::Gateway {
        &self.gateway
    }

    fn current_exclusions(&self) -> &[IpAddr] {
        &self.excluded
    }

    fn owned_exclusions(&self) -> Vec<IpAddr> {
        self.owned.clone()
    }

    fn reconcile_exclusions(&mut self, desired: &[IpAddr]) -> io::Result<()> {
        if self.fail_next_reconcile {
            self.fail_next_reconcile = false;
            self.owned.push(*desired.last().unwrap());
            return Err(io::Error::other("injected exclusion diff failure"));
        }
        self.owned = desired.to_vec();
        Ok(())
    }

    fn remove_owned_exclusions(&mut self) -> io::Result<()> {
        self.owned.clear();
        Ok(())
    }

    fn replace_egress(&mut self, egress: RouteInterface, gateway: Self::Gateway) -> io::Result<()> {
        self.egress = egress;
        self.gateway = gateway;
        Ok(())
    }

    fn install_exclusions(&mut self, excluded: &[IpAddr]) -> io::Result<()> {
        if self.fail_next_install {
            self.fail_next_install = false;
            return Err(io::Error::other("injected route installation failure"));
        }
        self.owned = excluded.to_vec();
        Ok(())
    }

    fn set_current_exclusions(&mut self, excluded: Vec<IpAddr>) {
        self.excluded = excluded;
    }
}

fn address(last: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(192, 0, 2, last))
}

fn state() -> FakeRouteState {
    FakeRouteState {
        egress: RouteInterface::new("physical0".to_owned(), 7).unwrap(),
        gateway: "192.0.2.1".to_owned(),
        excluded: vec![address(10)],
        owned: vec![address(10)],
        fail_next_install: false,
        fail_next_reconcile: false,
        route_missing: false,
        audit_fails: false,
        audits: 0,
    }
}

#[test]
fn target_transition_replaces_owned_route_state() {
    let mut state = state();
    let changed = reconcile_route_state(
        &mut state,
        RouteInterface::new("physical1".to_owned(), 8).unwrap(),
        "198.51.100.1".to_owned(),
        vec![address(11)],
    )
    .unwrap();

    assert!(changed);
    assert_eq!(state.egress.name(), "physical1");
    assert_eq!(state.gateway, "198.51.100.1");
    assert_eq!(state.excluded, vec![address(11)]);
    assert_eq!(state.owned, vec![address(11)]);
}

#[test]
fn failed_target_transition_restores_previous_working_state() {
    let mut state = state();
    state.fail_next_install = true;
    let error = reconcile_route_state(
        &mut state,
        RouteInterface::new("physical1".to_owned(), 8).unwrap(),
        "198.51.100.1".to_owned(),
        vec![address(11)],
    )
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("injected route installation failure"));
    assert_eq!(state.egress.name(), "physical0");
    assert_eq!(state.gateway, "192.0.2.1");
    assert_eq!(state.excluded, vec![address(10)]);
    assert_eq!(state.owned, vec![address(10)]);
}

#[test]
fn failed_exclusion_only_diff_restores_previous_working_state() {
    let mut state = state();
    state.fail_next_reconcile = true;

    let error = reconcile_route_state(
        &mut state,
        RouteInterface::new("physical0".to_owned(), 7).unwrap(),
        "192.0.2.1".to_owned(),
        vec![address(10), address(11)],
    )
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("injected exclusion diff failure"));
    assert_eq!(state.egress.name(), "physical0");
    assert_eq!(state.gateway, "192.0.2.1");
    assert_eq!(state.excluded, vec![address(10)]);
    assert_eq!(state.owned, vec![address(10)]);
}

#[test]
fn identical_interface_returning_still_repairs_missing_routes() {
    let mut state = state();
    state.route_missing = true;
    let changed = reconcile_route_state(
        &mut state,
        RouteInterface::new("physical0".into(), 7).unwrap(),
        "192.0.2.1".into(),
        vec![address(10)],
    )
    .unwrap();
    assert!(changed);
    assert!(!state.route_missing);
    assert_eq!(state.audits, 1);
    assert!(!reconcile_route_state(
        &mut state,
        RouteInterface::new("physical0".into(), 7).unwrap(),
        "192.0.2.1".into(),
        vec![address(10)]
    )
    .unwrap());
    assert_eq!(state.audits, 2);
}

#[test]
fn unchanged_topology_cannot_hide_failed_verification() {
    let mut state = state();
    state.audit_fails = true;
    assert!(reconcile_route_state(
        &mut state,
        RouteInterface::new("physical0".into(), 7).unwrap(),
        "192.0.2.1".into(),
        vec![address(10)]
    )
    .is_err());
    assert_eq!(state.audits, 1);
}
