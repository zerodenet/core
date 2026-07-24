use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::Path;

use serde::de::DeserializeOwned;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::super::model::ProductionGateCandidate;

const REQUIRED_FEATURES: [&str; 8] = [
    "status_api",
    "event_dispatcher",
    "panel_connector",
    "vless",
    "vmess",
    "trojan",
    "shadowsocks",
    "hysteria2",
];

pub(super) fn validate_build_info(
    text: &str,
    candidate: &ProductionGateCandidate,
) -> Result<(), String> {
    let fields = text
        .lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(key, value)| (key.trim(), value.trim()))
        .collect::<BTreeMap<_, _>>();
    require_equal(
        "build-info build_id",
        required_field(&fields, "build_id")?,
        &candidate.build_id,
    )?;
    require_equal(
        "build-info build_profile",
        required_field(&fields, "build_profile")?,
        "release",
    )?;
    require_equal(
        "build-info binary_sha256",
        required_field(&fields, "binary_sha256")?,
        &candidate.binary_sha256,
    )?;
    let git_hash = required_field(&fields, "git_hash")?;
    if !same_git_commit(git_hash, &candidate.git_hash) {
        return Err(format!(
            "build-info git_hash `{git_hash}` does not match candidate `{}`",
            candidate.git_hash
        ));
    }
    let features = required_field(&fields, "features")?
        .split(',')
        .map(str::trim)
        .filter(|feature| !feature.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    require_features("build-info features", &features)
}

fn required_field<'a>(fields: &'a BTreeMap<&str, &str>, name: &str) -> Result<&'a str, String> {
    fields
        .get(name)
        .copied()
        .ok_or_else(|| format!("build-info is missing `{name}`"))
}

pub(super) fn require_features_value(document: &Value, path: &[&str]) -> Result<(), String> {
    let features = json_array(document, path)?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("{} must contain only strings", path.join(".")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    require_features(&path.join("."), &features)
}

pub(super) fn require_features(label: &str, features: &[String]) -> Result<(), String> {
    let available = features.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let missing = REQUIRED_FEATURES
        .into_iter()
        .filter(|feature| !available.contains(feature))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{label} omitted required features: {}",
            missing.join(", ")
        ))
    }
}

pub(super) fn require_commit(
    document: &Value,
    path: &[&str],
    candidate: &ProductionGateCandidate,
) -> Result<(), String> {
    let value = json_str(document, path)?;
    if same_git_commit(value, &candidate.git_hash) {
        Ok(())
    } else {
        Err(format!(
            "{} `{value}` does not match candidate git_hash `{}`",
            path.join("."),
            candidate.git_hash
        ))
    }
}

pub(super) fn require_candidate_sha(
    document: &Value,
    path: &[&str],
    candidate: &ProductionGateCandidate,
) -> Result<(), String> {
    require_json_string(document, path, &candidate.binary_sha256)
}

pub(super) fn same_git_commit(left: &str, right: &str) -> bool {
    is_git_hash(left)
        && is_git_hash(right)
        && (left.eq_ignore_ascii_case(right)
            || left
                .to_ascii_lowercase()
                .starts_with(&right.to_ascii_lowercase())
            || right
                .to_ascii_lowercase()
                .starts_with(&left.to_ascii_lowercase()))
}

pub(super) fn require_git_hash(label: &str, value: &str) -> Result<(), String> {
    if is_git_hash(value) {
        Ok(())
    } else {
        Err(format!(
            "{label} must be a hexadecimal git hash of at least 7 characters"
        ))
    }
}

fn is_git_hash(value: &str) -> bool {
    value.len() >= 7 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) fn require_sha256(label: &str, value: &str) -> Result<(), String> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(format!("{label} must be a lowercase SHA-256 digest"))
    }
}

pub(super) fn require_nonempty(label: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("{label} must not be empty"))
    } else {
        Ok(())
    }
}

pub(super) fn require_equal(label: &str, actual: &str, expected: &str) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{label} must be `{expected}`, got `{actual}`"))
    }
}

pub(super) fn require_json_string(
    document: &Value,
    path: &[&str],
    expected: &str,
) -> Result<(), String> {
    require_equal(&path.join("."), json_str(document, path)?, expected)
}

pub(super) fn require_json_bool(
    document: &Value,
    path: &[&str],
    expected: bool,
) -> Result<(), String> {
    let actual = json_value(document, path)?
        .as_bool()
        .ok_or_else(|| format!("{} must be a boolean", path.join(".")))?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{} must be {expected}, got {actual}",
            path.join(".")
        ))
    }
}

pub(super) fn require_json_u64(
    document: &Value,
    path: &[&str],
    expected: u64,
) -> Result<(), String> {
    let actual = json_u64(document, path)?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{} must be {expected}, got {actual}",
            path.join(".")
        ))
    }
}

pub(super) fn require_json_u64_at_least(
    document: &Value,
    path: &[&str],
    minimum: u64,
) -> Result<(), String> {
    let actual = json_u64(document, path)?;
    if actual >= minimum {
        Ok(())
    } else {
        Err(format!(
            "{} must be at least {minimum}, got {actual}",
            path.join(".")
        ))
    }
}

pub(super) fn require_empty_array(document: &Value, path: &[&str]) -> Result<(), String> {
    let values = json_array(document, path)?;
    if values.is_empty() {
        Ok(())
    } else {
        Err(format!("{} must be empty", path.join(".")))
    }
}

pub(super) fn json_str<'a>(document: &'a Value, path: &[&str]) -> Result<&'a str, String> {
    json_value(document, path)?
        .as_str()
        .ok_or_else(|| format!("{} must be a string", path.join(".")))
}

pub(super) fn json_u64(document: &Value, path: &[&str]) -> Result<u64, String> {
    json_value(document, path)?
        .as_u64()
        .ok_or_else(|| format!("{} must be a non-negative integer", path.join(".")))
}

pub(super) fn json_f64(document: &Value, path: &[&str]) -> Result<f64, String> {
    json_value(document, path)?
        .as_f64()
        .ok_or_else(|| format!("{} must be a number", path.join(".")))
}

pub(super) fn json_array<'a>(document: &'a Value, path: &[&str]) -> Result<&'a [Value], String> {
    json_value(document, path)?
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| format!("{} must be an array", path.join(".")))
}

fn json_value<'a>(document: &'a Value, path: &[&str]) -> Result<&'a Value, String> {
    let mut current = document;
    for component in path {
        current = current
            .get(component)
            .ok_or_else(|| format!("evidence is missing `{}`", path.join(".")))?;
    }
    Ok(current)
}

pub(super) fn read_json(path: &Path, label: &'static str) -> Result<(Value, String), String> {
    let bytes = std::fs::read(path)
        .map_err(|error| format!("failed to read {label} `{}`: {error}", path.display()))?;
    let digest = format!("{:x}", Sha256::digest(&bytes));
    let document = serde_json::from_slice(&bytes)
        .map_err(|error| format!("failed to parse {label} `{}`: {error}", path.display()))?;
    Ok((document, digest))
}

pub(super) fn deserialize<T: DeserializeOwned>(value: Value, label: &str) -> Result<T, String> {
    serde_json::from_value(value).map_err(|error| format!("invalid {label}: {error}"))
}

pub(super) fn sha256_file(path: &Path) -> Result<String, String> {
    let mut file = File::open(path)
        .map_err(|error| format!("failed to open `{}`: {error}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("failed to read `{}`: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}
