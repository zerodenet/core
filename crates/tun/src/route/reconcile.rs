use std::io;
use std::net::IpAddr;

use super::RouteInterface;

pub(super) trait RouteReconcileState {
    type Gateway: Clone + PartialEq;

    /// Observe and repair OS routes even when the desired topology is unchanged.
    fn repair_routes(&mut self) -> io::Result<bool>;
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
    let changed =
        reconcile_desired_state(state, desired_egress, desired_gateway, desired_exclusions)?;
    // Desired state and the journal are intent/ownership, never proof that the
    // operating system retained a route across a link flap or sleep/wake.
    let repaired = state.repair_routes()?;
    Ok(changed || repaired)
}

fn reconcile_desired_state<T: RouteReconcileState>(
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
mod tests;
