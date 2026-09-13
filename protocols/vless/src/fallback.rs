//! Fallback selection mirrors the fixed Xray name/ALPN/path inheritance order.
use alloc::{collections::BTreeMap, string::String, vec::Vec};
use zero_traits::FallbackRule;

type Paths = BTreeMap<String, FallbackRule>;
type Alpns = BTreeMap<String, Paths>;
#[derive(Clone, Default)]
pub struct FallbackPolicy {
    names: BTreeMap<String, Alpns>,
}
impl FallbackPolicy {
    pub fn new(rules: Vec<FallbackRule>) -> Self {
        let mut names: BTreeMap<String, Alpns> = BTreeMap::new();
        for rule in rules {
            names
                .entry(rule.name.to_ascii_lowercase())
                .or_default()
                .entry(rule.alpn.to_ascii_lowercase())
                .or_default()
                .insert(rule.path.clone(), rule);
        }
        if let Some(defaults) = names.get("").cloned() {
            for (name, alpns) in &mut names {
                if !name.is_empty() {
                    for alpn in defaults.keys() {
                        alpns.entry(alpn.clone()).or_default();
                    }
                }
            }
        }
        for alpns in names.values_mut() {
            if let Some(defaults) = alpns.get("").cloned() {
                for (alpn, paths) in alpns {
                    if !alpn.is_empty() {
                        for (path, rule) in &defaults {
                            paths.entry(path.clone()).or_insert_with(|| rule.clone());
                        }
                    }
                }
            }
        }
        if let Some(defaults) = names.get("").cloned() {
            for (name, alpns) in &mut names {
                if name.is_empty() {
                    continue;
                }
                for (alpn, paths) in &defaults {
                    let target = alpns.entry(alpn.clone()).or_default();
                    for (path, rule) in paths {
                        target.entry(path.clone()).or_insert_with(|| rule.clone());
                    }
                }
            }
        }
        Self { names }
    }
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
    pub fn select(&self, name: &str, alpn: &str, first: &[u8]) -> Option<&FallbackRule> {
        let name = name.to_ascii_lowercase();
        let alpn = alpn.to_ascii_lowercase();
        let key = if self.names.contains_key(&name) {
            name.as_str()
        } else {
            self.names
                .keys()
                .filter(|key| !key.is_empty() && name.contains(key.as_str()))
                .max_by_key(|key| key.len())
                .map_or("", String::as_str)
        };
        let alpns = self.names.get(key).or_else(|| self.names.get(""))?;
        let paths = alpns.get(&alpn).or_else(|| alpns.get(""))?;
        paths
            .get(request_path(first).unwrap_or(""))
            .or_else(|| paths.get(""))
    }
}

fn request_path(first: &[u8]) -> Option<&str> {
    if first.len() < 18 || first[4] == b'*' {
        return None;
    }
    for i in 4..=8 {
        if first[i] == b'/' && first[i - 1] == b' ' {
            for j in i + 1..first.len().min(64) {
                match first[j] {
                    b'\r' | b'\n' => return None,
                    b'?' | b' ' => return core::str::from_utf8(&first[i..j]).ok(),
                    _ => {}
                }
            }
            break;
        }
    }
    None
}
