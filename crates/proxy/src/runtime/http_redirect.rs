pub(crate) fn select_redirect_destination(
    rules: &[zero_config::UrlRewriteRule],
    target: &zero_core::Address,
    port: u16,
) -> Option<(u16, String)> {
    let zero_core::Address::Domain(domain) = target else {
        return None;
    };

    for rule in rules {
        let Some(status) = rule.status_code else {
            continue;
        };
        let matched = if let Some(from) = &rule.from {
            from == domain
        } else if let Some(pattern) = &rule.from_regex {
            regex::Regex::new(pattern)
                .map(|re| re.is_match(domain))
                .unwrap_or(false)
        } else {
            false
        };

        if matched {
            return Some((status, format!("https://{}:{}", rule.to, port)));
        }
    }

    None
}
