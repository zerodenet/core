//! Freeze once under the actual map guard; serialize/write after unlocking.
use super::*;
use std::io::Write;

pub(super) struct Frame {
    pub(super) bytes: Vec<u8>,
    _snapshot: PersistedQuotaBook,
    // Both actual backings above are destroyed before returning their promise.
    _custody: memory::SnapshotCustody,
}
struct Count(usize);
impl Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self.0.checked_add(bytes.len()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "quota encoded length overflow")
        })?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Fixed<'a> {
    bytes: &'a mut Vec<u8>,
    length: usize,
}
impl Write for Fixed<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.length)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "quota serialization changed after preflight",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(super) fn encoded_row_bound(key: &QuotaKey) -> io::Result<usize> {
    #[derive(Serialize)]
    struct Borrowed<'a> { key: &'a QuotaKey, remaining_bytes: u64 }
    // Existing key/revision/initial fields never change in this row. Only the
    // remaining scalar changes; u64::MAX gives its exact largest digit count.
    let mut count = Count(0);
    serde_json::to_writer(&mut count, &Borrowed { key, remaining_bytes: u64::MAX }).map_err(io::Error::other)?;
    count.0.checked_add(1).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "quota row envelope overflow"))
}
pub(super) fn encode(book: &BookOwner) -> io::Result<Frame> {
    // The promise is declared before data for every allocation/error path.
    let custody;
    let snapshot = {
        let mut locked = book.lock().unwrap_or_else(|error| error.into_inner());
        custody = memory::SnapshotCustody::take_locked(book, &mut locked)?;
        let mut balances = Vec::new();
        balances
            .try_reserve_exact(locked.current.len())
            .map_err(io::Error::other)?;
        for current in locked.current.values() {
            if let Some(state) = locked.states.get(&current.key) {
                balances.push(PersistedQuotaBalance {
                    key: current.key.clone(),
                    remaining_bytes: state.remaining_bytes,
                });
            }
        }
        PersistedQuotaBook {
            version: 1,
            balances,
        }
    };
    let mut count = Count(0);
    serde_json::to_writer(&mut count, &snapshot)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(count.0).map_err(io::Error::other)?;
    serde_json::to_writer(
        &mut Fixed {
            bytes: &mut bytes,
            length: count.0,
        },
        &snapshot,
    )
    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(Frame {
        bytes,
        _snapshot: snapshot,
        _custody: custody,
    })
}
