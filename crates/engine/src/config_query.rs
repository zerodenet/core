//! Non-allocating projection of one actual immutable configuration generation.
use crate::{groups::GroupQueryBorrow, EngineRuntimeSnapshot, TargetId};
use serde::{ser::SerializeMap, Serialize};
use zero_api::{ApiError, ApiErrorCode, QueryRequest};
mod chains;
mod config;
mod policy;

#[derive(Clone, Copy)]
enum Kind {
    Config,
    Policies,
    Policy(TargetId),
}
/// Borrowed query values/real group guards. Use synchronously: measure, reserve
/// the actual response byte owner, encode from this same view, then release all
/// guards before awaiting IO/capacity or dropping a credit that can wake callers.
/// No DTO/cache/chain vectors are constructed by this projection.
pub struct EngineConfigQuery<'a> {
    snapshot: &'a EngineRuntimeSnapshot,
    state: GroupQueryBorrow<'a>,
    revision: u64,
    kind: Kind,
}
/// The original policy lookup failure, borrowing the already owned request.
/// Reserve its message storage before turning it into an owned ApiError.
#[derive(Debug)]
pub struct MissingPolicy<'a>(&'a str);
impl std::fmt::Debug for EngineConfigQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineConfigQuery").finish_non_exhaustive()
    }
}
impl MissingPolicy<'_> {
    pub fn message_len(&self) -> Option<usize> {
        self.0.len().checked_add("policy `` was not found".len())
    }
    pub fn message(&self) -> impl std::fmt::Display + '_ {
        PolicyMessage(self.0)
    }
    pub fn into_api_error(self) -> ApiError {
        use std::fmt::Write;
        let mut message = String::with_capacity(
            self.message_len()
                .expect("policy message length was prepaid"),
        );
        write!(&mut message, "{}", self.message()).expect("String formatting is infallible");
        ApiError::new(ApiErrorCode::NotFound, message)
    }
}
struct PolicyMessage<'a>(&'a str);
impl std::fmt::Display for PolicyMessage<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "policy `{}` was not found", self.0)
    }
}
impl EngineRuntimeSnapshot {
    /// None means this request belongs to another concrete query producer.
    /// Selected config/policy requests must handle the result of this exact
    /// snapshot, including MissingPolicy; do not re-query another generation.
    pub fn control_config_query<'a>(
        &'a self,
        request: &'a QueryRequest,
    ) -> Option<Result<EngineConfigQuery<'a>, MissingPolicy<'a>>> {
        let kind = match request {
            QueryRequest::Config(_) => Kind::Config,
            QueryRequest::Policies(_) => Kind::Policies,
            QueryRequest::Policy(query) => {
                if !self
                    .config()
                    .outbound_groups
                    .iter()
                    .any(|group| group.tag() == query.policy_tag)
                {
                    return Some(Err(MissingPolicy(&query.policy_tag)));
                }
                Kind::Policy(
                    self.plan()
                        .target_id(&query.policy_tag)
                        .expect("same actual configured policy"),
                )
            }
            _ => return None,
        };
        Some(Ok(EngineConfigQuery {
            snapshot: self,
            state: self.outbound_group_state.query_borrow(),
            revision: self.config_revision(),
            kind,
        }))
    }
}
impl EngineConfigQuery<'_> {
    /// HTTP's existing flat body; Serialize on this view itself retains the
    /// existing QueryResponse one-key envelope used by IPC and gRPC.
    pub fn body(&self) -> impl Serialize + '_ {
        Body(self)
    }
    fn tag(&self, id: TargetId) -> &str {
        self.snapshot
            .plan()
            .target(id)
            .expect("same actual target")
            .tag()
    }
}
impl Serialize for EngineConfigQuery<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let key = match self.kind {
            Kind::Config => "config",
            Kind::Policies => "policies",
            Kind::Policy(_) => "policy",
        };
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(key, &Body(self))?;
        map.end()
    }
}
struct Body<'v, 's>(&'v EngineConfigQuery<'s>);
impl Serialize for Body<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0.kind {
            Kind::Config => config::Config(self.0).serialize(serializer),
            Kind::Policies => policy::Policies(self.0).serialize(serializer),
            Kind::Policy(id) => policy::Policy(self.0, id).serialize(serializer),
        }
    }
}

#[cfg(test)]
#[path = "config_query/tests.rs"]
mod tests;
