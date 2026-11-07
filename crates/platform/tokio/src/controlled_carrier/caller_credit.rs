//! Exact original callee registrations and their independent Host ACK.
use super::slot::Slot;
use crate::contract::{scope::TaskJoinWitness, BoxFuture};
use std::sync::Arc;
fn required(slot: &Slot) -> [Option<TaskJoinWitness>; 2] {
    let state = slot.state();
    let source_owned = state
        .users
        .as_ref()
        .is_some_and(|users| users.packet_driver_custody());
    if source_owned {
        // The original slot has exactly these two possible callee duties, not
        // a new task list or the shared Source wire's lifetime. A duplicate
        // actual registration is observed once without allocating a Vec.
        let auxiliary = state.auxiliary.clone();
        let driver = state
            .driver
            .as_ref()
            .map(|(_, driver)| driver.clone())
            .filter(|driver| {
                auxiliary
                    .as_ref()
                    .is_none_or(|actual| !actual.same_registration(driver))
            });
        [auxiliary, driver]
    } else {
        let job_driver = state
            .users
            .as_ref()
            .filter(|users| {
                users
                    .preparation
                    .as_ref()
                    .is_some_and(|preparation| preparation.job.get().is_some())
            })
            .and_then(|_| state.driver.as_ref().map(|(_, driver)| driver.clone()));
        // Keep B's actual installed job registration even if its later seal
        // failed. Source-owned attempts above retain only their real callee
        // Auxiliary+driver duties; never the parent wire's self-join.
        [
            job_driver.or_else(|| state.users.as_ref().and_then(|users| users.credit_caller())),
            None,
        ]
    }
}

fn acknowledged(caller: &TaskJoinWitness) -> bool {
    caller
        .try_joined()
        .is_some_and(|joined| caller.matches_joined(&joined) && joined.registration_released())
}
pub(super) fn caller(slot: &Slot) -> Option<TaskJoinWitness> {
    required(slot)
        .into_iter()
        .flatten()
        .find(|witness| !acknowledged(witness))
}

#[derive(Default)]
struct Registrations {
    caller: Option<TaskJoinWitness>,
    auxiliary: Option<TaskJoinWitness>,
    driver: Option<TaskJoinWitness>,
    endpoint: Option<super::loan::CarrierEndpointUsersClosed>,
}
impl Registrations {
    fn snapshot(slot: &Slot) -> Option<Self> {
        let caller = caller(slot);
        let state = slot.state();
        // Unlike credit_caller, these exact native/driver registrations exist
        // even before users are installed or a logical loan is published.
        let endpoint = match (&state.endpoint_claim, &state.endpoint_closed) {
            (None, _) => None,
            (Some(_), Some(receipt)) => Some(receipt.clone()),
            (Some(_), None) => return None,
        };
        Some(Self {
            caller,
            endpoint,
            auxiliary: state.auxiliary.clone(),
            driver: state.driver.as_ref().map(|(_, actual)| actual.clone()),
        })
    }
    fn present(&self) -> bool {
        self.caller.is_some()
            || self.auxiliary.is_some()
            || self.driver.is_some()
            || self.endpoint.is_some()
    }
    fn released(&self) -> bool {
        self.caller
            .iter()
            .chain(self.auxiliary.iter())
            .chain(self.driver.iter())
            .all(|actual| {
                actual.try_joined().is_some_and(|joined| {
                    actual.matches_joined(&joined) && joined.registration_released()
                })
            })
            && self
                .endpoint
                .as_ref()
                .is_none_or(|receipt| receipt.acknowledged())
    }
    async fn wait(self) -> bool {
        for actual in self
            .caller
            .into_iter()
            .chain(self.auxiliary)
            .chain(self.driver)
        {
            let joined = actual.wait_registration_released().await;
            if !actual.matches_joined(&joined) || !joined.registration_released() {
                return false;
            }
        }
        if let Some(endpoint) = self.endpoint {
            if let Some(registrations) = endpoint.registrations() {
                for actual in registrations {
                    let joined = actual.wait_registration_released().await;
                    if !actual.matches_joined(&joined) || !joined.registration_released() {
                        return false;
                    }
                }
            }
        }
        true
    }
}
struct Credits {
    primary: Registrations,
    related: Option<Registrations>,
}
fn credits(slot: &Slot) -> Option<Credits> {
    let primary = Registrations::snapshot(slot)?;
    let related = { slot.state().related_child.clone() };
    // Follow only the actual already-linked child. The child's cleanup must
    // not wait its parent control driver: control closes after datagram views,
    // and reversing this dependency would wait for its own close continuation.
    let related = match related {
        Some(child) => Some(Registrations::snapshot(&child)?),
        None => None,
    };
    Some(Credits { primary, related })
}
impl Credits {
    fn released(&self) -> bool {
        self.primary.released() && self.related.as_ref().is_none_or(Registrations::released)
    }
    fn present(&self) -> bool {
        self.primary.present() || self.related.as_ref().is_some_and(Registrations::present)
    }
    async fn wait(self) -> bool {
        if !self.primary.wait().await {
            return false;
        }
        match self.related {
            Some(related) => related.wait().await,
            None => true,
        }
    }
}
pub(super) fn released(slot: &Slot) -> bool {
    credits(slot).is_some_and(|credits| credits.released())
}
/// One actual immutable witness observation in the original retirement row.
/// No Task/Scope, exit acceptance, native close or new deadline is created.
pub(super) fn observe(slot: &Slot) -> Option<BoxFuture<'static, bool>> {
    let actual = credits(slot)?;
    actual
        .present()
        .then(|| Box::pin(actual.wait()) as BoxFuture<'static, bool>)
}
pub(super) async fn wait(slot: &Arc<Slot>, deadline: tokio::time::Instant) -> bool {
    if released(slot) {
        return true;
    }
    let Some(actual) = credits(slot) else {
        return false;
    };
    tokio::time::timeout_at(deadline, actual.wait())
        .await
        .is_ok_and(|actual| actual)
        && released(slot)
}
