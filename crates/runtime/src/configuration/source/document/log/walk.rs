use super::*;
/// Sum escaped string spans, rather than max span: parent map parsers can
/// retain their key scratch while visiting a child. Unescaped borrowed strings
/// require none. Number scratch is conservatively covered independently.
pub(super) fn workspace(raw: &[u8]) -> io::Result<usize> {
    let (mut quoted, mut escaped, mut needs, mut begin, mut sum) = (false, false, false, 0, 0);
    for (at, b) in raw.iter().enumerate() {
        if !quoted {
            if *b == b'"' {
                quoted = true;
                begin = at;
                needs = false;
            }
            continue;
        }
        if escaped {
            escaped = false;
            continue;
        }
        match b {
            b'\\' => {
                escaped = true;
                needs = true;
            }
            b'"' => {
                quoted = false;
                if needs {
                    sum = add(sum, at - begin + 1)?;
                }
            }
            _ => {}
        }
    }
    if quoted && needs {
        sum = add(sum, raw.len() - begin)?;
    }
    let numeric = super::super::super::super::control_budget::control_json_decoder_workspace(raw)?;
    add(twice(sum)?, numeric)
}
pub(super) fn validate(raw: &RawValue, maximum: usize, depth: usize) -> io::Result<()> {
    if depth >= 128 {
        return Err(invalid("recursion limit exceeded"));
    }
    if magic(raw)? {
        let (value, _) = materialize(raw, maximum)?;
        drop(value);
        return Ok(());
    }
    let mut failure = None;
    let result = serde::Deserializer::deserialize_any(
        &mut serde_json::Deserializer::from_str(raw.get()),
        Walk {
            maximum,
            depth,
            failure: &mut failure,
        },
    );
    if let Some(error) = failure {
        return Err(error);
    }
    result.map_err(invalid)
}
struct Walk<'a> {
    maximum: usize,
    depth: usize,
    failure: &'a mut Option<io::Error>,
}
impl Walk<'_> {
    fn child<E: serde::de::Error>(&mut self, raw: &RawValue) -> Result<(), E> {
        match validate(raw, self.maximum, self.depth + 1) {
            Ok(()) => Ok(()),
            Err(error) => {
                *self.failure = Some(error);
                Err(E::custom("actual Value subtree rejected"))
            }
        }
    }
}
impl<'de> Visitor<'de> for Walk<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JSON value")
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<(), E> {
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(mut self, mut a: A) -> Result<(), A::Error> {
        while a.next_key::<serde::de::IgnoredAny>()?.is_some() {
            let raw = a.next_value::<&RawValue>()?;
            self.child(raw)?;
        }
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(mut self, mut a: A) -> Result<(), A::Error> {
        while let Some(raw) = a.next_element::<&RawValue>()? {
            self.child(raw)?;
        }
        Ok(())
    }
}
