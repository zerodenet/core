use std::collections::BTreeSet;

use serde_json::Value;

use super::super::model::ProductionGateCandidate;
use super::helpers::{
    json_array, json_f64, json_str, json_u64, require_candidate_sha, require_commit,
    require_empty_array, require_features_value, require_json_bool, require_json_string,
    require_json_u64, require_json_u64_at_least, require_nonempty, require_sha256,
    validate_build_info,
};

const REQUIRED_CONFORMANCE_CHECKS: [&str; 7] = [
    "registration",
    "sync",
    "commands",
    "node_config",
    "users",
    "traffic",
    "alive",
];

pub(super) fn validate_qualification(
    document: &Value,
    candidate: &ProductionGateCandidate,
) -> Result<String, String> {
    require_json_string(document, &["schema_id"], "zero.connector.qualification.v4")?;
    require_json_string(document, &["evidence_grade"], "release_candidate")?;
    require_json_bool(document, &["production_gate", "requested"], true)?;
    require_json_bool(document, &["production_gate", "passed"], true)?;
    require_empty_array(document, &["production_gate", "failures"])?;
    require_json_bool(document, &["source", "dirty_before"], false)?;
    require_json_bool(document, &["source", "dirty_after"], false)?;
    require_commit(document, &["source", "commit_before"], candidate)?;
    require_commit(document, &["source", "commit_after"], candidate)?;
    require_commit(document, &["candidate", "git_hash"], candidate)?;
    require_json_string(document, &["candidate", "build_profile"], "release")?;
    require_candidate_sha(document, &["candidate", "sha256_before"], candidate)?;
    require_candidate_sha(document, &["candidate", "sha256_after"], candidate)?;
    require_candidate_sha(document, &["candidate", "self_reported_sha256"], candidate)?;
    require_json_string(
        document,
        &["candidate", "native_contract_id"],
        "zero.panel.v1",
    )?;
    require_features_value(document, &["candidate", "features"])?;

    let build_info = json_str(document, &["candidate", "build_info"])?;
    validate_build_info(build_info, candidate)?;
    require_json_u64_at_least(document, &["event_count"], 100_000)?;
    require_json_u64_at_least(document, &["restart_cycles"], 10)?;
    require_json_u64_at_least(document, &["outage_event_count"], 10_000)?;
    require_json_u64_at_least(document, &["minimum_duration_seconds"], 3_600)?;
    require_json_u64(document, &["exit_code"], 0)?;
    require_json_bool(document, &["soak", "rss_supported"], true)?;
    let elapsed = json_f64(document, &["soak", "elapsed_seconds"])?;
    if elapsed < 3_600.0 {
        return Err(format!(
            "qualification soak.elapsed_seconds must be at least 3600, got {elapsed}"
        ));
    }
    let peak_rss = json_u64(document, &["soak", "peak_rss_bytes"])?;
    let max_rss = json_u64(document, &["production_gate", "max_peak_rss_bytes"])?;
    if max_rss == 0 || peak_rss > max_rss {
        return Err(format!(
            "qualification peak RSS {peak_rss} exceeds or lacks approved limit {max_rss}"
        ));
    }

    let before = json_str(document, &["candidate", "native_contract_sha256_before"])?;
    let after = json_str(document, &["candidate", "native_contract_sha256_after"])?;
    require_sha256("qualification native contract SHA-256", before)?;
    super::helpers::require_equal(
        "qualification native contract SHA-256 before/after",
        before,
        after,
    )?;
    Ok(before.to_owned())
}

pub(super) fn validate_conformance(
    document: &Value,
    candidate: &ProductionGateCandidate,
) -> Result<(), String> {
    require_json_string(document, &["schema_id"], "zero.panel.conformance-report.v2")?;
    require_json_string(document, &["contract_id"], "zero.panel.v1")?;
    require_json_bool(document, &["complete"], true)?;
    require_nonempty("conformance node_id", json_str(document, &["node_id"])?)?;
    require_commit(document, &["candidate", "git_hash"], candidate)?;
    require_json_string(document, &["candidate", "build_id"], &candidate.build_id)?;
    require_json_string(document, &["candidate", "build_profile"], "release")?;
    require_candidate_sha(document, &["candidate", "binary_sha256"], candidate)?;
    require_features_value(document, &["candidate", "features"])?;

    let checks = json_array(document, &["checks"])?;
    let mut names = BTreeSet::new();
    for check in checks {
        let name = json_str(check, &["name"])?;
        require_json_string(check, &["status"], "passed")?;
        if !names.insert(name.to_owned()) {
            return Err(format!("conformance check `{name}` appears more than once"));
        }
    }
    let expected = REQUIRED_CONFORMANCE_CHECKS
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    if names != expected {
        return Err(format!(
            "conformance checks do not match the Zero native contract: expected {expected:?}, got {names:?}"
        ));
    }
    Ok(())
}

pub(super) fn validate_upgrade_preflight(
    document: &Value,
    candidate: &ProductionGateCandidate,
    contract_sha256: &str,
) -> Result<(), String> {
    require_json_string(
        document,
        &["schema_id"],
        "zero.connector.upgrade-preflight.v2",
    )?;
    require_json_string(document, &["evidence_grade"], "release_candidate")?;
    require_json_bool(document, &["production_gate", "requested"], true)?;
    require_json_bool(document, &["production_gate", "passed"], true)?;
    require_json_bool(document, &["compatible"], true)?;
    require_json_bool(document, &["same_binary_allowed"], false)?;
    require_candidate_sha(document, &["candidate", "sha256"], candidate)?;
    require_candidate_sha(document, &["candidate", "self_reported_sha256"], candidate)?;
    require_json_string(
        document,
        &["candidate", "native_contract_id"],
        "zero.panel.v1",
    )?;
    require_json_string(
        document,
        &["candidate", "native_contract_sha256"],
        contract_sha256,
    )?;
    let previous_sha = json_str(document, &["previous", "sha256"])?;
    require_sha256("upgrade previous SHA-256", previous_sha)?;
    if previous_sha == candidate.binary_sha256 {
        return Err("upgrade preflight used the candidate as the previous binary".to_owned());
    }
    validate_build_info(json_str(document, &["candidate", "build_info"])?, candidate)?;
    require_features_value(
        document,
        &["production_gate", "required_candidate_features"],
    )
}
