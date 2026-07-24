#![cfg(feature = "panel_connector")]

use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use zero_connector::{verify_production_gate, ProductionGateCandidate, ProductionGateReport};

const CANDIDATE_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const PREVIOUS_SHA: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const CONTRACT_SHA: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const FEATURES: [&str; 8] = [
    "status_api",
    "event_dispatcher",
    "panel_connector",
    "vless",
    "vmess",
    "trojan",
    "shadowsocks",
    "hysteria2",
];

#[test]
fn complete_native_evidence_for_the_same_candidate_passes() {
    let fixture = EvidenceFixture::new();
    let report = verify(&fixture).expect("production gate");

    assert!(report.passed);
    assert_eq!(report.schema_id, "zero.connector.production-gate.v1");
    assert_eq!(report.checks.len(), 5);
    assert!(report.checks.iter().all(|check| check.status == "passed"));
    assert_eq!(report.evidence.len(), 5);
}

#[test]
fn candidate_hash_mismatch_fails_closed() {
    let mut fixture = EvidenceFixture::new();
    fixture.conformance["candidate"]["binary_sha256"] = Value::String(PREVIOUS_SHA.to_owned());
    fixture.write();

    let error = verify(&fixture).unwrap_err();
    assert!(error.contains("candidate.binary_sha256"));
    assert!(error.contains(CANDIDATE_SHA));
}

#[test]
fn missing_business_approval_fails_closed() {
    let mut fixture = EvidenceFixture::new();
    fixture.approval["approvals"]
        .as_array_mut()
        .expect("approvals")
        .retain(|approval| approval["role"] != "billing_business");
    fixture.write();

    let error = verify(&fixture).unwrap_err();
    assert!(error.contains("approval roles must be exactly"));
}

#[test]
fn non_native_contract_result_cannot_replace_native_conformance() {
    let mut fixture = EvidenceFixture::new();
    fixture.conformance = json!({
        "schema_id": "zero.panel.xboard-compatibility.v1",
        "complete": true,
        "candidate": {
            "git_hash": "0123456789abcdef",
            "binary_sha256": CANDIDATE_SHA
        }
    });
    fixture.write();

    let error = verify(&fixture).unwrap_err();
    assert!(error.contains("zero.panel.conformance-report.v2"));
}

#[test]
fn declared_upgrade_artifact_hash_must_match_the_real_file() {
    let mut fixture = EvidenceFixture::new();
    fixture.live_upgrade["artifacts"][1]["sha256"] = Value::String(CONTRACT_SHA.to_owned());
    fixture.write();

    let error = verify(&fixture).unwrap_err();
    assert!(error.contains("previous_state_after_candidate"));
    assert!(error.contains("SHA-256"));
}

struct EvidenceFixture {
    directory: tempfile::TempDir,
    qualification: Value,
    conformance: Value,
    upgrade: Value,
    live_upgrade: Value,
    approval: Value,
}

impl EvidenceFixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("evidence tempdir");
        let mut fixture = Self {
            directory,
            qualification: qualification(),
            conformance: conformance(),
            upgrade: upgrade(),
            live_upgrade: live_upgrade(),
            approval: approval(),
        };
        for name in [
            "previous-zero.exe",
            "previous-state-after-candidate.json",
            "billing-reconciliation.json",
        ] {
            std::fs::write(fixture.path(name), []).expect("write evidence artifact");
        }
        fixture.write();
        fixture
    }

    fn paths(&self) -> [PathBuf; 5] {
        [
            self.path("qualification.json"),
            self.path("conformance.json"),
            self.path("upgrade.json"),
            self.path("live-upgrade.json"),
            self.path("approval.json"),
        ]
    }

    fn write(&mut self) {
        for (name, document) in [
            ("qualification.json", &self.qualification),
            ("conformance.json", &self.conformance),
            ("upgrade.json", &self.upgrade),
            ("live-upgrade.json", &self.live_upgrade),
            ("approval.json", &self.approval),
        ] {
            std::fs::write(
                self.path(name),
                serde_json::to_vec_pretty(document).expect("serialize evidence"),
            )
            .expect("write evidence");
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.directory.path().join(name)
    }
}

fn verify(fixture: &EvidenceFixture) -> Result<ProductionGateReport, String> {
    let paths = fixture.paths();
    verify_production_gate(
        candidate(),
        paths.each_ref().map(|path| Path::new(path.as_os_str())),
    )
}

fn candidate() -> ProductionGateCandidate {
    ProductionGateCandidate {
        build_id: "1.2.3".to_owned(),
        git_hash: "0123456789abcdef".to_owned(),
        build_profile: "release".to_owned(),
        features: FEATURES.into_iter().map(str::to_owned).collect(),
        binary_sha256: CANDIDATE_SHA.to_owned(),
    }
}

fn build_info() -> String {
    format!(
        "build_id: 1.2.3\nbuild_profile: release\nfeatures: {}\nbinary_sha256: {CANDIDATE_SHA}\ngit_hash: 0123456789abcdef",
        FEATURES.join(",")
    )
}

fn qualification() -> Value {
    json!({
        "schema_id": "zero.connector.qualification.v4",
        "evidence_grade": "release_candidate",
        "source": {
            "commit_before": "0123456789abcdef0123456789abcdef01234567",
            "commit_after": "0123456789abcdef0123456789abcdef01234567",
            "dirty_before": false,
            "dirty_after": false
        },
        "candidate": {
            "sha256_before": CANDIDATE_SHA,
            "sha256_after": CANDIDATE_SHA,
            "build_info": build_info(),
            "git_hash": "0123456789abcdef",
            "build_profile": "release",
            "features": FEATURES,
            "self_reported_sha256": CANDIDATE_SHA,
            "native_contract_id": "zero.panel.v1",
            "native_contract_sha256_before": CONTRACT_SHA,
            "native_contract_sha256_after": CONTRACT_SHA
        },
        "production_gate": {
            "requested": true,
            "passed": true,
            "failures": [],
            "max_peak_rss_bytes": 134217728
        },
        "event_count": 100000,
        "restart_cycles": 10,
        "minimum_duration_seconds": 3600,
        "outage_event_count": 10000,
        "soak": {
            "elapsed_seconds": 3600.25,
            "peak_rss_bytes": 67108864,
            "rss_supported": true
        },
        "exit_code": 0
    })
}

fn conformance() -> Value {
    json!({
        "schema_id": "zero.panel.conformance-report.v2",
        "contract_id": "zero.panel.v1",
        "node_id": "production-candidate-node",
        "candidate": {
            "build_id": "1.2.3",
            "git_hash": "0123456789abcdef",
            "build_profile": "release",
            "features": FEATURES,
            "binary_sha256": CANDIDATE_SHA
        },
        "complete": true,
        "checks": [
            {"name": "registration", "status": "passed", "detail": "ok"},
            {"name": "sync", "status": "passed", "detail": "ok"},
            {"name": "commands", "status": "passed", "detail": "ok"},
            {"name": "node_config", "status": "passed", "detail": "ok"},
            {"name": "users", "status": "passed", "detail": "ok"},
            {"name": "traffic", "status": "passed", "detail": "ok"},
            {"name": "alive", "status": "passed", "detail": "ok"}
        ]
    })
}

fn upgrade() -> Value {
    json!({
        "schema_id": "zero.connector.upgrade-preflight.v2",
        "evidence_grade": "release_candidate",
        "previous": {
            "sha256": PREVIOUS_SHA
        },
        "candidate": {
            "sha256": CANDIDATE_SHA,
            "build_info": build_info(),
            "self_reported_sha256": CANDIDATE_SHA,
            "native_contract_id": "zero.panel.v1",
            "native_contract_sha256": CONTRACT_SHA
        },
        "production_gate": {
            "requested": true,
            "passed": true,
            "required_candidate_features": FEATURES
        },
        "same_binary_allowed": false,
        "compatible": true
    })
}

fn live_upgrade() -> Value {
    json!({
        "schema_id": "zero.connector.production-upgrade-report.v1",
        "evidence_grade": "release_candidate",
        "completed_at": "2026-07-24T12:00:00Z",
        "previous": {
            "build_id": "1.2.2",
            "git_hash": "fedcba9876543210",
            "binary_sha256": PREVIOUS_SHA
        },
        "candidate": {
            "build_id": "1.2.3",
            "git_hash": "0123456789abcdef",
            "binary_sha256": CANDIDATE_SHA
        },
        "production_gate": {"passed": true},
        "checks": {
            "previous_state_backup_created": true,
            "previous_process_gracefully_stopped": true,
            "candidate_started": true,
            "candidate_state_compatible": true,
            "node_revision_observed": true,
            "user_revision_acknowledged": true,
            "command_replay_verified": true,
            "quota_checkpoint_verified": true,
            "traffic_ack_verified": true,
            "previous_read_candidate_state": true,
            "rollback_completed": true,
            "billing_reconciled": true
        },
        "artifacts": [
            {
                "name": "previous_binary",
                "path": "previous-zero.exe",
                "sha256": PREVIOUS_SHA
            },
            {
                "name": "previous_state_after_candidate",
                "path": "previous-state-after-candidate.json",
                "sha256": PREVIOUS_SHA
            },
            {
                "name": "billing_reconciliation",
                "path": "billing-reconciliation.json",
                "sha256": PREVIOUS_SHA
            }
        ]
    })
}

fn approval() -> Value {
    json!({
        "schema_id": "zero.connector.production-approval.v1",
        "candidate": {
            "build_id": "1.2.3",
            "git_hash": "0123456789abcdef",
            "binary_sha256": CANDIDATE_SHA
        },
        "decision": "passed",
        "approved_at": "2026-07-24T13:00:00Z",
        "approvals": [
            {
                "role": "development",
                "approver": "developer",
                "approved_at": "2026-07-24T12:30:00Z",
                "decision": "approved"
            },
            {
                "role": "operations",
                "approver": "operator",
                "approved_at": "2026-07-24T12:40:00Z",
                "decision": "approved"
            },
            {
                "role": "billing_business",
                "approver": "billing owner",
                "approved_at": "2026-07-24T12:50:00Z",
                "decision": "approved"
            }
        ],
        "remaining_risks": []
    })
}
