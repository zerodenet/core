use std::path::Path;

use helpers::{
    deserialize, read_json, require_equal, require_features, require_git_hash, require_nonempty,
    require_sha256,
};

use super::model::{
    ApprovalReport, LiveUpgradeReport, ProductionGateCandidate, ProductionGateCheck,
    ProductionGateEvidence, ProductionGateReport,
};

mod helpers;
mod manual;
mod native;

pub(super) fn verify(
    candidate: ProductionGateCandidate,
    paths: [&Path; 5],
) -> Result<ProductionGateReport, String> {
    validate_candidate(&candidate)?;
    let (qualification, qualification_digest) = read_json(paths[0], "qualification manifest")?;
    let (conformance, conformance_digest) = read_json(paths[1], "conformance report")?;
    let (upgrade, upgrade_digest) = read_json(paths[2], "upgrade preflight manifest")?;
    let (live_value, live_digest) = read_json(paths[3], "live upgrade report")?;
    let (approval_value, approval_digest) = read_json(paths[4], "approval report")?;

    let contract_sha256 = native::validate_qualification(&qualification, &candidate)?;
    native::validate_conformance(&conformance, &candidate)?;
    native::validate_upgrade_preflight(&upgrade, &candidate, &contract_sha256)?;
    let live: LiveUpgradeReport = deserialize(live_value, "live upgrade report")?;
    manual::validate_live_upgrade(&live, &candidate, paths[3])?;
    let approval: ApprovalReport = deserialize(approval_value, "approval report")?;
    manual::validate_approval(&approval, &candidate)?;

    Ok(ProductionGateReport {
        schema_id: "zero.connector.production-gate.v1",
        candidate,
        passed: true,
        checks: vec![
            passed(
                "qualification",
                "release-candidate soak, outage, restart, RSS and native contract evidence matched",
            ),
            passed(
                "reference_conformance",
                "all seven Zero native panel contract checks passed for this candidate",
            ),
            passed(
                "upgrade_preflight",
                "distinct release binaries and persistent state formats were compatible",
            ),
            passed(
                "live_upgrade",
                "graceful upgrade, rollback and billing reconciliation were recorded",
            ),
            passed(
                "approval",
                "development, operations and billing/business approvals matched the candidate",
            ),
        ],
        evidence: vec![
            evidence("qualification", paths[0], qualification_digest),
            evidence("reference_conformance", paths[1], conformance_digest),
            evidence("upgrade_preflight", paths[2], upgrade_digest),
            evidence("live_upgrade", paths[3], live_digest),
            evidence("approval", paths[4], approval_digest),
        ],
    })
}

fn validate_candidate(candidate: &ProductionGateCandidate) -> Result<(), String> {
    require_equal(
        "candidate build_profile",
        &candidate.build_profile,
        "release",
    )?;
    require_sha256("candidate binary_sha256", &candidate.binary_sha256)?;
    require_nonempty("candidate build_id", &candidate.build_id)?;
    require_git_hash("candidate git_hash", &candidate.git_hash)?;
    require_features("candidate features", &candidate.features)
}

fn passed(name: &'static str, detail: impl Into<String>) -> ProductionGateCheck {
    ProductionGateCheck {
        name,
        status: "passed",
        detail: detail.into(),
    }
}

fn evidence(kind: &'static str, path: &Path, sha256: String) -> ProductionGateEvidence {
    ProductionGateEvidence {
        kind,
        path: path.display().to_string(),
        sha256,
    }
}
