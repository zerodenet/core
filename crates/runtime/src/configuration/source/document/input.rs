//! One original inode, captured length, inert facts, rewind, bounded bytes.
use super::*;
use std::io::{Read, Seek, SeekFrom};

pub(super) fn read(
    source: &ConfigurationSource,
    path: &Path,
    dialect: probe::Dialect,
) -> io::Result<(String, probe::Facts)> {
    // These values outlive with_file's registry lock, on success and failure.
    // A failed native close cannot drop a returned large buffer under that lock.
    let mut text = String::new();
    let mut facts = probe::Facts::default();
    source.io.with_file(
        || std::fs::File::open(path),
        |file| {
            let captured = file.metadata()?.len();
            validate_utf8(&mut (&mut *file).take(captured))?;
            file.seek(SeekFrom::Start(0))?;
            facts = probe::read(&mut (&mut *file).take(captured), dialect)?;
            let length = usize::try_from(captured).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "initial document length overflow",
                )
            })?;
            if length > facts.file {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "initial document exceeds its actual file_read_max_bytes",
                ));
            }
            text.try_reserve_exact(length).map_err(io::Error::other)?;
            file.seek(SeekFrom::Start(0))?;
            (&mut *file).take(captured).read_to_string(&mut text)?;
            Ok(())
        },
    )?;
    // Same-inode content may have been modified while reading. Acceptance
    // and logging projection use the facts of the bytes actually returned,
    // never only the earlier observation. No second file/FD is opened.
    facts = probe::read(&mut text.as_bytes(), dialect)?;
    if text.len() > facts.file {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "initial document exceeds its returned file_read_max_bytes",
        ));
    }
    Ok((text, facts))
}

fn validate_utf8(reader: &mut dyn Read) -> io::Result<()> {
    let mut bytes = [0; 4096 + 3];
    let mut tail = 0;
    loop {
        let count = reader.read(&mut bytes[tail..4096])?;
        if count == 0 {
            return if tail == 0 {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "initial document is not UTF-8",
                ))
            };
        }
        let end = tail + count;
        match std::str::from_utf8(&bytes[..end]) {
            Ok(_) => tail = 0,
            Err(error) if error.error_len().is_none() => {
                tail = end - error.valid_up_to();
                bytes.copy_within(error.valid_up_to()..end, 0);
            }
            Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
        }
    }
}
