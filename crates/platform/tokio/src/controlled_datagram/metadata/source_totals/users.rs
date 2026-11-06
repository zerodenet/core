//! The runtime's original issuer/transfer/confirmation book, not a second list.
use super::super::users::MetadataUsersClosedReceipt;
use super::*;
use crate::contract::scope::TaskRegistrar;
#[derive(Default)]
pub struct MetadataSourceUsers {
    claims: Mutex<Vec<Claim>>,
    pub(super) changed: tokio::sync::Notify,
}
struct Claim {
    issuer: MetadataViewIssuer,
    transferred: bool,
    confirmed: Option<MetadataUsersClosedReceipt>,
}
impl MetadataSourceUsers {
    pub fn retain(&self, issuer: MetadataViewIssuer) -> usize {
        let mut claims = self
            .claims
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let index = claims.len();
        claims.push(Claim {
            issuer,
            transferred: false,
            confirmed: None,
        });
        index
    }
    pub fn transferred(&self, index: usize) {
        self.claims
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)[index]
            .transferred = true;
    }
    pub fn confirm(&self, receipt: &MetadataUsersClosedReceipt, tasks: &TaskRegistrar) -> bool {
        {
            let mut claims = self
                .claims
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(claim) = claims
                .iter_mut()
                .find(|claim| claim.transferred && claim.issuer.validates_users(receipt, tasks))
            else {
                return false;
            };
            claim.confirmed.get_or_insert_with(|| receipt.clone());
        }
        self.changed.notify_waiters();
        true
    }
    pub fn pending(&self) -> usize {
        self.claims
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|claim| claim.transferred && claim.confirmed.is_none())
            .count()
    }
    pub fn confirmed(&self) -> Vec<MetadataUsersClosedReceipt> {
        self.claims
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter_map(|claim| claim.confirmed.clone())
            .collect()
    }
}
