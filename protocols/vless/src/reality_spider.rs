//! Protocol-owned spiderX interpretation; no HTTP execution or control plane.
use alloc::{format, string::String, vec::Vec};
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Profile {
    pub path: String,
    pub ranges: [(u32, u32); 5],
}
impl Profile {
    pub fn parse(value: &str) -> Result<Self, String> {
        let value = if value.is_empty() { "/" } else { value };
        if !value.starts_with('/')
            || value.starts_with("//")
            || value.len() > 8192
            || value.bytes().any(|b| b < 32 || b == 127)
        {
            return Err("invalid REALITY spider_x path".into());
        }
        let mut url = url::Url::parse(&format!("https://spider.invalid{value}"))
            .map_err(|_| "invalid REALITY spider_x URL")?;
        let mut ranges = [(0, 0); 5];
        let keys = ["p", "c", "t", "i", "r"];
        let limits = [65536, 64, 256, 60000, 60000];
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        let pairs: Vec<_> = url
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let mut seen = [false; 5];
        for (key, value) in pairs {
            if let Some(index) = keys.iter().position(|k| *k == key) {
                if seen[index] || value.is_empty() {
                    continue;
                }
                seen[index] = true;
                let (from, to) = value.split_once('-').unwrap_or((&value, &value));
                let from: u32 = from.parse().map_err(|_| "invalid REALITY spider_x range")?;
                let to: u32 = to.parse().map_err(|_| "invalid REALITY spider_x range")?;
                if from > to || to > limits[index] {
                    return Err("REALITY spider_x range exceeds bounded navigation limits".into());
                }
                ranges[index] = (from, to);
            } else {
                query.append_pair(&key, &value);
            }
        }
        if ranges[1].1.saturating_mul(ranges[2].1) > 4096 {
            return Err("REALITY spider_x request budget exceeds 4096".into());
        }
        let query = query.finish();
        url.set_query((!query.is_empty()).then_some(query.as_str()));
        // URI fragments are not sent in HTTP requests.
        let path = format!(
            "{}{}",
            url.path(),
            url.query().map(|q| format!("?{q}")).unwrap_or_default()
        );
        Ok(Self { path, ranges })
    }
}
