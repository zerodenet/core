use super::*;

#[derive(Debug)]
struct Cause(Option<Box<Cause>>);
impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original cause")
    }
}
impl Error for Cause {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.as_deref().map(|source| source as &dyn Error)
    }
}

#[test]
fn retained_causes_are_visible_and_bounded() {
    let leaf = Cause(None);
    assert!(Sources(&leaf).to_string().is_empty());
    let chain = Cause(Some(Box::new(leaf)));
    assert_eq!(Sources(&chain).to_string(), "original cause");
    let deep = (0..20).fold(chain, |source, _| Cause(Some(Box::new(source))));
    let rendered = Sources(&deep).to_string();
    assert_eq!(rendered.matches("original cause").count(), 16);
    assert!(rendered.ends_with("further causes omitted"));
}
