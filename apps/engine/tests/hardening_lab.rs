//! Opt-in, resource-bounded local regression repros. These are intentionally known-red on the
//! installed durable-streams 0.1.5 provider; see `.agent/REPORT.md`. They do not run in the ordinary
//! fast suite and are not release qualification. Run each explicitly, serially:
//! `cargo test -p electric-circuits-engine --features test-support --test hardening_lab -- --ignored --test-threads=1`
//!
//! The Python runner owns loopback PG/DS/engine processes, tracks PIDs, checks the shared 8 GB / 12 GB
//! disk limits, and removes each fixture. No credentials, downloads, or external services are used.

use std::path::PathBuf;
use std::process::Command;

fn probe(script: &str, args: &[&str]) -> serde_json::Value {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = Command::new("python3")
        .arg("-B")
        .arg(root.join("scripts/lab").join(script))
        .args(args)
        .arg("--engine")
        .arg(env!("CARGO_BIN_EXE_electric-circuits-engine"))
        .output()
        .expect("start the local hardening runner");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let artifacts = stdout.lines().find_map(|line| line.strip_prefix("Artifacts: ")).unwrap_or_else(|| {
        panic!("probe did not produce a result: {stdout}\n{}", String::from_utf8_lossy(&output.stderr))
    });
    let result =
        std::fs::read_to_string(PathBuf::from(artifacts).join("result.json")).expect("read local probe result");
    let result: serde_json::Value = serde_json::from_str(&result).expect("decode local probe result");
    assert_eq!(result["all_children_reaped"], true, "cleanup is part of the contract: {result}");
    result
}

#[test]
#[ignore = "known-red real DS 500 MB append; local lab only, needs installed PG18/DS and disk budget"]
fn a_shape_sized_500_mb_atomic_append_is_accepted_by_the_real_store() {
    let result = probe("append_limit.py", &["--mb", "500"]);
    assert_eq!(result["status"], 204, "valid atomic shape output must land: {result}");
}

#[test]
#[ignore = "known-red 200 MB catalog restart on unpaged DS 0.1.5; local lab only, needs PG18/DS"]
fn a_200_mb_churn_catalog_restores_the_original_native_streams() {
    let result = probe("hardening.py", &["catalog", "--mb", "200"]);
    assert_eq!(result["outcome"], "pass", "restart must restore the existing streams and match SQL: {result}");
}
