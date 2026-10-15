//! Bounded scratch allocation for control-plane event encoding.
use serde::Serialize;
use std::io;
pub(crate) fn serialized_bytes<T: Serialize>(value: &T) -> u64 {
    struct Count(u64);
    impl io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len() as u64);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, value).expect("event serialization");
    count.0
}
pub(crate) fn to_vec<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    let mut bytes = Vec::with_capacity((serialized_bytes(value) as usize).saturating_add(1));
    serde_json::to_writer(&mut bytes, value)?;
    Ok(bytes)
}
