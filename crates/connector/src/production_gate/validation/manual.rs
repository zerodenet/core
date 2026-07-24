use std::collections::BTreeSet;
use std::path::{Component, Path};

use super::super::model::{
    ApprovalReport, LiveUpgradeReport, ProductionGateCandidate, ReleaseIdentity,
};
use super::helpers::{
    require_equal, require_git_hash, require_nonempty, require_sha256, same_git_commit, sha256_file,
};

pub(super) fn validate_live_upgrade(
    report: &LiveUpgradeReport,
    candidate: &ProductionGateCandidate,
    report_path: &Path,
) -> Result<(), String> {
    require_equal(
        "live upgrade schema_id",
        &report.schema_id,
        "zero.connector.production-upgrade-report.v1",
    )?;
    require_equal(
        "live upgrade evidence_grade",
        &report.evidence_grade,
        "release_candidate",
    )?;
    require_nonempty("live upgrade completed_at", &report.completed_at)?;
    if !report.production_gate.passed {
        return Err("live upgrade production_gate.passed is false".to_owned());
    }
    validate_release_identity("live upgrade candidate", &report.candidate, candidate)?;
    require_sha256(
        "live upgrade previous binary_sha256",
        &report.previous.binary_sha256,
    )?;
    require_nonempty("live upgrade previous build_id", &report.previous.build_id)?;
    require_git_hash("live upgrade previous git_hash", &report.previous.git_hash)?;
    if report.previous.binary_sha256 == candidate.binary_sha256 {
        return Err("live upgrade previous and candidate binaries are identical".to_owned());
    }
    let missing = report.checks.missing();
    if !missing.is_empty() {
        return Err(format!(
            "live upgrade report has failed or missing checks: {}",
            missing.join(", ")
        ));
    }
    let evidence_directory = report_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .map_err(|error| {
            format!(
                "failed to resolve live upgrade evidence directory `{}`: {error}",
                report_path.display()
            )
        })?;
    let mut artifact_names = BTreeSet::new();
    for artifact in &report.artifacts {
        require_nonempty("live upgrade artifact name", &artifact.name)?;
        require_nonempty("live upgrade artifact path", &artifact.path)?;
        require_sha256("live upgrade artifact SHA-256", &artifact.sha256)?;
        if !artifact_names.insert(artifact.name.clone()) {
            return Err(format!(
                "live upgrade artifact `{}` appears more than once",
                artifact.name
            ));
        }
        let relative = Path::new(&artifact.path);
        if relative.is_absolute()
            || relative
                .components()
                .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
        {
            return Err(format!(
                "live upgrade artifact `{}` must use a contained relative path",
                artifact.name
            ));
        }
        let resolved = evidence_directory
            .join(relative)
            .canonicalize()
            .map_err(|error| {
                format!(
                    "failed to resolve live upgrade artifact `{}` at `{}`: {error}",
                    artifact.name,
                    relative.display()
                )
            })?;
        if !resolved.starts_with(&evidence_directory) {
            return Err(format!(
                "live upgrade artifact `{}` escapes the evidence directory",
                artifact.name
            ));
        }
        let actual_sha256 = sha256_file(&resolved)?;
        require_equal(
            &format!("live upgrade artifact `{}` SHA-256", artifact.name),
            &actual_sha256,
            &artifact.sha256,
        )?;
    }
    let required_artifacts = [
        "billing_reconciliation",
        "previous_binary",
        "previous_state_after_candidate",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    if !required_artifacts.is_subset(&artifact_names) {
        let expected = required_artifacts.iter().collect::<BTreeSet<_>>();
        let actual = artifact_names.iter().collect::<BTreeSet<_>>();
        return Err(format!(
            "live upgrade artifacts must include {expected:?}, got {actual:?}"
        ));
    }
    let previous_binary = report
        .artifacts
        .iter()
        .find(|artifact| artifact.name == "previous_binary")
        .expect("required artifact name checked");
    require_equal(
        "previous binary artifact SHA-256",
        &previous_binary.sha256,
        &report.previous.binary_sha256,
    )?;
    if report
        .notes
        .as_deref()
        .is_some_and(|notes| notes.trim().is_empty())
    {
        return Err("live upgrade notes must be omitted rather than empty".to_owned());
    }
    Ok(())
}

pub(super) fn validate_approval(
    report: &ApprovalReport,
    candidate: &ProductionGateCandidate,
) -> Result<(), String> {
    require_equal(
        "approval schema_id",
        &report.schema_id,
        "zero.connector.production-approval.v1",
    )?;
    require_equal("approval decision", &report.decision, "passed")?;
    require_nonempty("approval approved_at", &report.approved_at)?;
    validate_release_identity("approval candidate", &report.candidate, candidate)?;

    let mut roles = BTreeSet::new();
    for approval in &report.approvals {
        require_nonempty("approval role", &approval.role)?;
        require_nonempty("approval approver", &approval.approver)?;
        if ["required", "todo", "tbd"]
            .iter()
            .any(|placeholder| approval.approver.eq_ignore_ascii_case(placeholder))
        {
            return Err(format!(
                "approval role `{}` still contains placeholder approver `{}`",
                approval.role, approval.approver
            ));
        }
        require_nonempty("approval approved_at", &approval.approved_at)?;
        require_equal(
            "individual approval decision",
            &approval.decision,
            "approved",
        )?;
        if !roles.insert(approval.role.as_str()) {
            return Err(format!(
                "approval role `{}` appears more than once",
                approval.role
            ));
        }
    }
    let expected = ["billing_business", "development", "operations"]
        .into_iter()
        .collect::<BTreeSet<_>>();
    if roles != expected {
        return Err(format!(
            "approval roles must be exactly {expected:?}, got {roles:?}"
        ));
    }
    for risk in &report.remaining_risks {
        require_nonempty("approval remaining risk", risk)?;
    }
    Ok(())
}

fn validate_release_identity(
    label: &str,
    actual: &ReleaseIdentity,
    candidate: &ProductionGateCandidate,
) -> Result<(), String> {
    require_equal(
        &format!("{label} build_id"),
        &actual.build_id,
        &candidate.build_id,
    )?;
    if !same_git_commit(&actual.git_hash, &candidate.git_hash) {
        return Err(format!(
            "{label} git_hash `{}` does not match candidate `{}`",
            actual.git_hash, candidate.git_hash
        ));
    }
    require_equal(
        &format!("{label} binary_sha256"),
        &actual.binary_sha256,
        &candidate.binary_sha256,
    )
}
