use super::*;
use std::{collections::HashMap, sync::Mutex as StdMutex, time::Instant};

pub(super) struct Paths {
    values: Vec<String>,
}
impl Paths {
    pub(super) fn choose(&self) -> String {
        self.values[rand::random_range(0..self.values.len())].clone()
    }
    pub(super) fn discover(&mut self, origin: &str, body: &[u8]) {
        let body = String::from_utf8_lossy(body);
        for part in body.split("href=\"").skip(1) {
            let Some((href, _)) = part.split_once('"') else {
                continue;
            };
            let path = href.strip_prefix(origin).unwrap_or(href);
            if !path.starts_with('/')
                || path.starts_with("//")
                || path.contains('.')
                || path.len() > 8192
                || path.bytes().any(|b| b < 32 || b == 127)
                || self.values.iter().any(|p| p == path)
            {
                continue;
            }
            if self.values.len() >= 256 {
                break;
            }
            self.values.push(path.to_owned());
        }
    }
}
type Cache = HashMap<String, (Instant, Arc<Mutex<Paths>>)>;
pub(super) fn for_host(host: &str, first: &str) -> Arc<Mutex<Paths>> {
    static CACHE: OnceLock<StdMutex<Cache>> = OnceLock::new();
    let mut cache = CACHE.get_or_init(Default::default).lock().unwrap();
    cache.retain(|_, (time, _)| time.elapsed() < Duration::from_secs(300));
    if let Some((_, paths)) = cache.get(host) {
        return paths.clone();
    }
    if cache.len() >= 1024 {
        if let Some(oldest) = cache
            .iter()
            .min_by_key(|(_, (time, _))| *time)
            .map(|(host, _)| host.clone())
        {
            cache.remove(&oldest);
        }
    }
    let paths = Arc::new(Mutex::new(Paths {
        values: vec![first.to_owned()],
    }));
    cache.insert(host.to_owned(), (Instant::now(), paths.clone()));
    paths
}
