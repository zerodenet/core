use super::*;

#[test]
fn pf_policy_ends_in_a_quick_block() {
    let rules = policy_rules(
        "utun8",
        &["203.0.113.0/24".parse().unwrap()],
        &["192.0.2.1".parse().unwrap()],
    );
    let uid = unsafe { libc::geteuid() };
    assert!(rules.contains("pass out quick on utun8 all"));
    assert!(rules.contains(&format!("pass out quick all user {uid}\n")));
    assert!(!rules.contains("pass out quick user"));
    assert!(rules.contains("pass out quick to 127.0.0.0/8\n"));
    assert!(rules.contains("pass out quick to ::1/128\n"));
    assert!(rules.contains("pass out quick to 192.0.2.1"));
    assert!(rules.ends_with("block drop out quick to 203.0.113.0/24\n"));
}

#[test]
fn pf_policy_exempts_loopback_destinations_before_protected_routes() {
    let rules = policy_rules(
        "utun8",
        &[
            "0.0.0.0/1".parse().unwrap(),
            "128.0.0.0/1".parse().unwrap(),
            "::/1".parse().unwrap(),
            "8000::/1".parse().unwrap(),
        ],
        &[],
    );
    let ipv4_pass = rules.find("pass out quick to 127.0.0.0/8").unwrap();
    let ipv6_pass = rules.find("pass out quick to ::1/128").unwrap();
    let first_block = rules.find("block drop out quick").unwrap();
    assert!(ipv4_pass < first_block);
    assert!(ipv6_pass < first_block);
    let blocked = rules
        .lines()
        .filter_map(|line| line.strip_prefix("block drop out quick to "))
        .map(|prefix| prefix.parse::<IpNet>().unwrap())
        .collect::<Vec<_>>();
    assert!(!blocked
        .iter()
        .any(|prefix| prefix.contains(&"127.0.0.1".parse::<IpAddr>().unwrap())));
    assert!(!blocked
        .iter()
        .any(|prefix| prefix.contains(&"::1".parse::<IpAddr>().unwrap())));
}

#[test]
fn main_rules_must_unconditionally_evaluate_the_anchor_namespace() {
    assert!(evaluates_anchor_namespace(
        "scrub-anchor \"com.apple/*\" all\nanchor \"com.apple/*\" all\n"
    ));
    assert!(evaluates_anchor_namespace(
        "  anchor \"com.apple/*\"  all  "
    ));
    for rules in [
        "# anchor \"com.apple/*\" all",
        "scrub-anchor \"com.apple/*\" all",
        "anchor \"com.apple/other\" all",
        "anchor \"com.apple/*\"",
        "anchor \"com.apple/*\" in all",
        "anchor \"com.apple/*\" out all",
        "anchor \"com.apple/*\" on lo0 all",
        "anchor \"com.apple/*\" inet all",
        "anchor \"com.apple/*\" inet6 all",
        "anchor \"com.apple/*\" from any to 192.0.2.0/24",
        "anchor \"com.apple/*\" from 192.0.2.0/24 to any",
    ] {
        assert!(!evaluates_anchor_namespace(rules), "accepted: {rules}");
    }
}
