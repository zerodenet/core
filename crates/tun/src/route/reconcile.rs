use std::io;
use std::net::IpAddr;

use super::RouteInterface;

pub(super) trait RouteReconcileState {
    type Gateway: Clone + PartialEq;

    fn current_egress(&self) -> &RouteInterface;
    fn current_gateway(&self) -> &Self::Gateway;
    fn current_exclusions(&self) -> &[IpAddr];
    fn owned_exclusions(&self) -> Vec<IpAddr>;
    fn reconcile_exclusions(&mut self, desired: &[IpAddr]) -> io::Result<()>;
    fn remove_owned_exclusions(&mut self) -> io::Result<()>;
    fn replace_egress(&mut self, egress: RouteInterface, gateway: Self::Gateway) -> io::Result<()>;
    fn install_exclusions(&mut self, excluded: &[IpAddr]) -> io::Result<()>;
    fn set_current_exclusions(&mut self, excluded: Vec<IpAddr>);
}

pub(super) fn reconcile_route_state<T: RouteReconcileState>(
    state: &mut T,
    desired_egress: RouteInterface,
    desired_gateway: T::Gateway,
    desired_exclusions: Vec<IpAddr>,
) -> io::Result<bool> {
    let target_changed =
        state.current_egress() != &desired_egress || state.current_gateway() != &desired_gateway;
    if !target_changed && state.current_exclusions() == desired_exclusions {
        return Ok(false);
    }
    if !target_changed {
        let old_exclusions = state.current_exclusions().to_vec();
        let old_owned = state.owned_exclusions();
        if let Err(error) = state.reconcile_exclusions(&desired_exclusions) {
            let rollback = (|| {
                state.remove_owned_exclusions()?;
                state.install_exclusions(&old_owned)?;
                state.set_current_exclusions(old_exclusions);
                Ok::<_, io::Error>(())
            })();
            return Err(with_rollback_error(error, rollback));
        }
        state.set_current_exclusions(desired_exclusions);
        return Ok(true);
    }

    let old_egress = state.current_egress().clone();
    let old_gateway = state.current_gateway().clone();
    let old_exclusions = state.current_exclusions().to_vec();
    let old_owned = state.owned_exclusions();
    let apply = (|| {
        state.remove_owned_exclusions()?;
        state.replace_egress(desired_egress, desired_gateway)?;
        state.install_exclusions(&desired_exclusions)?;
        state.set_current_exclusions(desired_exclusions);
        Ok(())
    })();
    if let Err(error) = apply {
        let rollback = (|| {
            state.remove_owned_exclusions()?;
            state.replace_egress(old_egress, old_gateway)?;
            state.install_exclusions(&old_owned)?;
            state.set_current_exclusions(old_exclusions);
            Ok::<_, io::Error>(())
        })();
        return Err(with_rollback_error(error, rollback));
    }
    Ok(true)
}

pub(super) fn with_rollback_error(error: io::Error, rollback: io::Result<()>) -> io::Error {
    match rollback {
        Ok(()) => error,
        Err(rollback) => io::Error::new(
            error.kind(),
            format!("route reconciliation failed ({error}); rollback also failed ({rollback})"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    struct FakeRouteState {
        egress: RouteInterface,
        gateway: String,
        excluded: Vec<IpAddr>,
        owned: Vec<IpAddr>,
        fail_next_install: bool,
        fail_next_reconcile: bool,
    }

    impl RouteReconcileState for FakeRouteState {
        type Gateway = String;

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

        fn replace_egress(
            &mut self,
            egress: RouteInterface,
            gateway: Self::Gateway,
        ) -> io::Result<()> {
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
}
