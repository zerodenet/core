use super::*;

const TABLE: &str = "zero_killswitch_tunroutereconcilee2e";

pub(super) fn assert_rule_loss_recovers(namespace: &str, binary: &str, socket: &Path) {
    let expected = snapshot(namespace).expect("installed nftables kill switch");
    for (damage, manual) in [("single-rule", false), ("chain", true), ("table", true)] {
        match damage {
            "single-rule" => {
                let rules = checked_nft(
                    namespace,
                    &[
                        "--json", "--handle", "list", "chain", "inet", TABLE, "output",
                    ],
                );
                let rules: serde_json::Value = serde_json::from_slice(&rules).unwrap();
                let handle = rules["nftables"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find_map(|entry| {
                        let rule = &entry["rule"];
                        rule["expr"].as_array().and_then(|expressions| {
                            expressions
                                .iter()
                                .any(|expr| expr.get("reject").is_some())
                                .then(|| rule["handle"].as_u64().unwrap())
                        })
                    })
                    .expect("protected-prefix reject rule");
                checked_nft(
                    namespace,
                    &[
                        "delete",
                        "rule",
                        "inet",
                        TABLE,
                        "output",
                        "handle",
                        &handle.to_string(),
                    ],
                );
            }
            "chain" => {
                checked_nft(namespace, &["flush", "chain", "inet", TABLE, "output"]);
            }
            "table" => {
                checked_nft(namespace, &["delete", "table", "inet", TABLE]);
            }
            _ => unreachable!(),
        }
        // Mutation success proves damage was applied. The watcher may already
        // have repaired it before another read, which is valid fast recovery.
        if manual {
            let result = netns_command(
                namespace,
                binary,
                &["tun", "recover", "--socket", path(socket)],
            );
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
        }
        let deadline = Instant::now() + Duration::from_secs(40);
        while snapshot(namespace).as_ref() != Some(&expected) {
            assert!(
                Instant::now() < deadline,
                "nftables {damage} loss was not repaired (manual={manual})"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        assert_eq!(
            wait_for_healthy_egress(namespace, binary, socket, Some("physical0")),
            "physical0"
        );
    }
}

fn snapshot(namespace: &str) -> Option<Vec<u8>> {
    let output = netns_command(
        namespace,
        "nft",
        &["--stateless", "--numeric", "list", "table", "inet", TABLE],
    );
    output.status.success().then_some(output.stdout)
}

fn checked_nft(namespace: &str, arguments: &[&str]) -> Vec<u8> {
    let output = netns_command(namespace, "nft", arguments);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}
