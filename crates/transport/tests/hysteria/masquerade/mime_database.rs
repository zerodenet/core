use super::*;

#[test]
fn glob_database_retains_builtin_and_first_weighted_type() {
    let mut db = Database::builtins();
    db.globs(std::io::Cursor::new(
        "# database\n80:application/x-ignored:*.html\n80:application/x-first:*.mine\n50:application/x-second:*.mine\n50:application/x-glob:*.bad[0-9]\n50:text/x-special:*.T1\n",
    ));
    assert_eq!(db.lookup("html"), Some("text/html; charset=utf-8"));
    assert_eq!(db.lookup("mine"), Some("application/x-first"));
    assert_eq!(db.lookup("MINE"), Some("application/x-first"));
    assert_eq!(db.lookup("bad[0-9]"), None);
    assert_eq!(db.lookup("t1"), Some("text/x-special; charset=utf-8"));
}

#[test]
fn types_database_overrides_builtin_and_preserves_exact_case() {
    let mut db = Database::builtins();
    db.types(std::io::Cursor::new(
        "# comment\napplication/x-html html\ntext/x-note Note # not-an-extension\napplication/x-other note\ninvalid nope\n",
    ));
    assert_eq!(db.lookup("html"), Some("application/x-html"));
    assert_eq!(db.lookup("Note"), Some("text/x-note; charset=utf-8"));
    assert_eq!(db.lookup("note"), Some("application/x-other"));
    assert_eq!(db.lookup("NOTE"), Some("application/x-other"));
    assert_eq!(db.lookup("not-an-extension"), None);
    assert_eq!(db.lookup("nope"), None);
}

#[test]
fn first_readable_glob_database_prevents_types_fallback() {
    let root = std::env::temp_dir().join(format!("zero-mime-db-{}", rand::random::<u64>()));
    std::fs::create_dir(&root).unwrap();
    let missing = root.join("missing");
    let globs = root.join("globs2");
    let types = root.join("mime.types");
    std::fs::write(&globs, "50:application/x-glob:*.mine\n").unwrap();
    std::fs::write(&types, "application/x-fallback mine\n").unwrap();
    let db = Database::from_paths(&[&missing, &globs], &[&types]);
    assert_eq!(db.lookup("mine"), Some("application/x-glob"));
    let db = Database::from_paths(&[&missing], &[&types]);
    assert_eq!(db.lookup("mine"), Some("application/x-fallback"));
    std::fs::remove_dir_all(root).unwrap();
}
