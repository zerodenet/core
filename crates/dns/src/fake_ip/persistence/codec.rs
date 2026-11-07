//! Borrowed records: measure and encode the same values without a JSON Vec.
use super::*;
use std::io::Write;
#[derive(Serialize)]
#[serde(tag = "record", rename_all = "snake_case")]
pub(in crate::fake_ip) enum RecordRef<'a> {
    Header {
        schema: &'a str,
        #[serde(flatten)]
        metadata: &'a PersistenceMetadata,
    },
    Upsert {
        domain: &'a str,
        ip: IpAddr,
        expires_at_unix_ms: u64,
    },
    Retire {
        ip: IpAddr,
        reusable_after_unix_ms: u64,
    },
}
pub(in crate::fake_ip) fn write_record(
    writer: &mut dyn Write,
    record: &impl Serialize,
) -> io::Result<()> {
    serde_json::to_writer(&mut *writer, record).map_err(io::Error::other)?;
    writer.write_all(b"\n")
}
pub(in crate::fake_ip) fn measured(
    mut encode: impl FnMut(&mut dyn Write) -> io::Result<()>,
) -> io::Result<usize> {
    let mut count = Count::default();
    encode(&mut count)?;
    Ok(count.0)
}
pub(in crate::fake_ip) fn rewrite(
    journal: &JournalGrant,
    metadata: &PersistenceMetadata,
    mappings: &[PersistedMapping],
    retired: &[PersistedRetiredAddress],
) -> io::Result<usize> {
    let encode = |writer: &mut dyn Write| {
        write_record(
            writer,
            &RecordRef::Header {
                schema: SCHEMA,
                metadata,
            },
        )?;
        for mapping in mappings {
            write_record(
                writer,
                &RecordRef::Upsert {
                    domain: &mapping.domain,
                    ip: mapping.ip,
                    expires_at_unix_ms: mapping.expires_at_unix_ms,
                },
            )?;
        }
        for address in retired {
            write_record(
                writer,
                &RecordRef::Retire {
                    ip: address.ip,
                    reusable_after_unix_ms: address.reusable_after_unix_ms,
                },
            )?;
        }
        Ok(())
    };
    let bytes = measured(encode)?;
    journal.replace(bytes, encode)?;
    Ok(bytes)
}
#[derive(Default)]
struct Count(usize);
impl Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self.0.checked_add(bytes.len()).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::FileTooLarge,
                "Fake-IP encoded length overflow",
            )
        })?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
