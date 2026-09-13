//! Host extension database, loaded once with the standard library's precedence.
use std::{collections::HashMap, io::BufRead, path::Path, sync::OnceLock};

#[derive(Default)]
pub(super) struct Database {
    exact: HashMap<String, String>,
    folded: HashMap<String, String>,
}
impl Database {
    fn builtins() -> Self {
        let mut db = Self::default();
        for extension in super::BUILTIN_EXTENSIONS {
            let value = super::builtin(extension).unwrap().to_owned();
            db.exact.insert((*extension).to_owned(), value.clone());
            db.folded.insert((*extension).to_owned(), value);
        }
        db
    }
    fn insert(&mut self, extension: &str, value: &str) {
        let Ok(parsed) = value.parse::<::mime::Mime>() else {
            return;
        };
        let value = if parsed.type_() == ::mime::TEXT && parsed.get_param(::mime::CHARSET).is_none()
        {
            format!("{parsed}; charset=utf-8")
        } else {
            value.to_owned()
        };
        self.exact.insert(extension.to_owned(), value.clone());
        self.folded.insert(extension.to_lowercase(), value);
    }
    pub(super) fn lookup(&self, extension: &str) -> Option<&str> {
        self.exact
            .get(extension)
            .or_else(|| self.folded.get(&extension.to_lowercase()))
            .map(String::as_str)
    }
    fn globs(&mut self, input: impl BufRead) {
        for line in input.lines().map_while(Result::ok) {
            let fields: Vec<_> = line.split(':').collect();
            if fields.len() < 3 || fields[0].is_empty() || fields[0].starts_with('#') {
                continue;
            }
            let Some(extension) = fields[2].strip_prefix("*.") else {
                continue;
            };
            if extension.is_empty()
                || extension.contains(['?', '*', '['])
                || self.exact.contains_key(extension)
            {
                continue;
            }
            self.insert(extension, fields[1]);
        }
    }
    fn types(&mut self, input: impl BufRead) {
        for line in input.lines().map_while(Result::ok) {
            let mut fields = line.split_whitespace();
            let Some(value) = fields.next().filter(|s| !s.starts_with('#')) else {
                continue;
            };
            for extension in fields.take_while(|s| !s.starts_with('#')) {
                self.insert(extension, value);
            }
        }
    }
    fn from_paths(globs: &[&Path], types: &[&Path]) -> Self {
        let mut db = Self::builtins();
        for path in globs {
            if let Ok(file) = std::fs::File::open(path) {
                db.globs(std::io::BufReader::new(file));
                return db;
            }
        }
        for path in types {
            if let Ok(file) = std::fs::File::open(path) {
                db.types(std::io::BufReader::new(file));
            }
        }
        db
    }
}

pub(super) fn database() -> &'static Database {
    static DATABASE: OnceLock<Database> = OnceLock::new();
    DATABASE.get_or_init(|| {
        #[cfg(unix)]
        {
            Database::from_paths(
                &[
                    Path::new("/usr/local/share/mime/globs2"),
                    Path::new("/usr/share/mime/globs2"),
                ],
                &[
                    Path::new("/etc/mime.types"),
                    Path::new("/etc/apache2/mime.types"),
                    Path::new("/etc/apache/mime.types"),
                    Path::new("/etc/httpd/conf/mime.types"),
                ],
            )
        }
        #[cfg(windows)]
        {
            let mut db = Database::builtins();
            let root = winreg::RegKey::predef(winreg::enums::HKEY_CLASSES_ROOT);
            for name in root.enum_keys().filter_map(Result::ok) {
                let Some(extension) = name.strip_prefix('.').filter(|s| !s.is_empty()) else {
                    continue;
                };
                let Ok(key) = root.open_subkey(&name) else {
                    continue;
                };
                let Ok(value) = key.get_value::<String, _>("Content Type") else {
                    continue;
                };
                if extension == "js"
                    && matches!(value.as_str(), "text/plain" | "text/plain; charset=utf-8")
                {
                    continue;
                }
                db.insert(extension, &value);
            }
            db
        }
        #[cfg(not(any(unix, windows)))]
        Database::builtins()
    })
}

#[cfg(test)]
#[path = "../../../../../tests/hysteria/masquerade/mime_database.rs"]
mod tests;
