use std::process::Command;

fn main() {
    emit_git_rerun_paths();

    // Embed build timestamp using the `time` crate for correct formatting.
    let now = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown".to_owned());
    println!("cargo:rustc-env=ZERO_BUILD_TIME={now}");
    if let Ok(profile) = std::env::var("PROFILE") {
        println!("cargo:rustc-env=ZERO_BUILD_PROFILE={profile}");
    }

    // Embed git commit hash if available.
    if let Some(hash) = git_output(&["rev-parse", "--short", "HEAD"]) {
        println!("cargo:rustc-env=ZERO_GIT_HASH={hash}");
    }

    // Embed git tag if available.
    if let Some(tag) = git_output(&["describe", "--tags", "--always"]) {
        println!("cargo:rustc-env=ZERO_GIT_DESCRIBE={tag}");
    }
}

fn emit_git_rerun_paths() {
    if let Some(head_path) = git_output(&["rev-parse", "--git-path", "HEAD"]) {
        println!("cargo:rerun-if-changed={head_path}");
    }
    if let Some(symbolic_ref) = git_output(&["symbolic-ref", "-q", "HEAD"]) {
        if let Some(ref_path) = git_output(&["rev-parse", "--git-path", &symbolic_ref]) {
            println!("cargo:rerun-if-changed={ref_path}");
        }
    }
}

fn git_output(arguments: &[&str]) -> Option<String> {
    let output = Command::new("git").args(arguments).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (!value.is_empty()).then_some(value)
}
