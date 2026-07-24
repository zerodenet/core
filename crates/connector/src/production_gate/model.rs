use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionGateCandidate {
    pub build_id: String,
    pub git_hash: String,
    pub build_profile: String,
    pub features: Vec<String>,
    pub binary_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionGateEvidence {
    pub kind: &'static str,
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionGateCheck {
    pub name: &'static str,
    pub status: &'static str,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionGateReport {
    pub schema_id: &'static str,
    pub candidate: ProductionGateCandidate,
    pub passed: bool,
    pub checks: Vec<ProductionGateCheck>,
    pub evidence: Vec<ProductionGateEvidence>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LiveUpgradeReport {
    pub schema_id: String,
    pub evidence_grade: String,
    pub completed_at: String,
    pub previous: ReleaseIdentity,
    pub candidate: ReleaseIdentity,
    pub production_gate: ManualGate,
    pub checks: LiveUpgradeChecks,
    pub artifacts: Vec<ArtifactEvidence>,
    #[serde(default)]
    pub notes: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReleaseIdentity {
    pub build_id: String,
    pub git_hash: String,
    pub binary_sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ManualGate {
    pub passed: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LiveUpgradeChecks {
    pub previous_state_backup_created: bool,
    pub previous_process_gracefully_stopped: bool,
    pub candidate_started: bool,
    pub candidate_state_compatible: bool,
    pub node_revision_observed: bool,
    pub user_revision_acknowledged: bool,
    pub command_replay_verified: bool,
    pub quota_checkpoint_verified: bool,
    pub traffic_ack_verified: bool,
    pub previous_read_candidate_state: bool,
    pub rollback_completed: bool,
    pub billing_reconciled: bool,
}

impl LiveUpgradeChecks {
    pub(super) fn missing(&self) -> Vec<&'static str> {
        [
            (
                self.previous_state_backup_created,
                "previous_state_backup_created",
            ),
            (
                self.previous_process_gracefully_stopped,
                "previous_process_gracefully_stopped",
            ),
            (self.candidate_started, "candidate_started"),
            (
                self.candidate_state_compatible,
                "candidate_state_compatible",
            ),
            (self.node_revision_observed, "node_revision_observed"),
            (
                self.user_revision_acknowledged,
                "user_revision_acknowledged",
            ),
            (self.command_replay_verified, "command_replay_verified"),
            (self.quota_checkpoint_verified, "quota_checkpoint_verified"),
            (self.traffic_ack_verified, "traffic_ack_verified"),
            (
                self.previous_read_candidate_state,
                "previous_read_candidate_state",
            ),
            (self.rollback_completed, "rollback_completed"),
            (self.billing_reconciled, "billing_reconciled"),
        ]
        .into_iter()
        .filter_map(|(passed, name)| (!passed).then_some(name))
        .collect()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ArtifactEvidence {
    pub name: String,
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ApprovalReport {
    pub schema_id: String,
    pub candidate: ReleaseIdentity,
    pub decision: String,
    pub approved_at: String,
    pub approvals: Vec<Approval>,
    #[serde(default)]
    pub remaining_risks: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Approval {
    pub role: String,
    pub approver: String,
    pub approved_at: String,
    pub decision: String,
}
