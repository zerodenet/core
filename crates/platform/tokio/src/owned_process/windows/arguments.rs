//! Preserve ProcessRequest's ordinary Windows Command argument semantics.
//! UTF-16 code units are retained, including unpaired surrogates in OsString.
use std::{ffi::OsStr, io, os::windows::ffi::OsStrExt};
pub(super) fn wide(value: &OsStr) -> io::Result<Vec<u16>> {
    let value: Vec<_> = value.encode_wide().collect();
    if value.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "process launch contains an interior NUL",
        ));
    }
    Ok(value)
}
pub(super) fn executable(
    program: &OsStr,
    arguments: &[std::ffi::OsString],
) -> io::Result<Vec<u16>> {
    let mut result = vec![34];
    result.extend(wide(program)?);
    result.push(34);
    for argument in arguments {
        result.push(32);
        append(&mut result, &wide(argument)?, false)?;
    }
    result.push(0);
    Ok(result)
}
pub(super) fn batch(program: &[u16], arguments: &[std::ffi::OsString]) -> io::Result<Vec<u16>> {
    let program = program.strip_suffix(&[0]).unwrap_or(program);
    if program.contains(&34) || program.last() == Some(&92) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid batch program path",
        ));
    }
    let mut result: Vec<_> = "cmd.exe /e:ON /v:OFF /d /c \"\"".encode_utf16().collect();
    result.extend_from_slice(program);
    result.push(34);
    for argument in arguments {
        result.push(32);
        append(&mut result, &wide(argument)?, true)?;
    }
    result.extend_from_slice(&[34, 0]);
    Ok(result)
}
fn append(result: &mut Vec<u16>, argument: &[u16], batch: bool) -> io::Result<()> {
    if batch && argument.iter().any(|unit| matches!(*unit, 10 | 13)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "batch file arguments contain a newline",
        ));
    }
    let quotes = argument.is_empty()
        || if batch {
            argument.last() == Some(&92)
                || argument.iter().any(|unit| {
                    *unit < 32
                        || (127..=159).contains(unit)
                        || (*unit < 128
                            && !(*unit as u8).is_ascii_alphanumeric()
                            && !b"#$*+-./:?@\\_".contains(&(*unit as u8)))
                })
        } else {
            argument.iter().any(|unit| matches!(*unit, 32 | 9))
        };
    if quotes {
        result.push(34);
    }
    let mut slashes = 0;
    for unit in argument {
        if *unit == 92 {
            slashes += 1;
        } else {
            if *unit == 34 {
                result.extend(std::iter::repeat_n(92, slashes));
                result.push(if batch { 34 } else { 92 });
            } else if batch && *unit == 37 {
                // Match Command's non-expanding batch argument representation.
                result.extend("%%cd:~,".encode_utf16());
            }
            slashes = 0;
        }
        result.push(*unit);
    }
    if quotes {
        result.extend(std::iter::repeat_n(92, slashes));
        result.push(34);
    }
    Ok(())
}
