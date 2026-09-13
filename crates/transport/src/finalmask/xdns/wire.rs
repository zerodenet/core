//! Bounded DNS message encoding, including compression and opaque TXT RDATA.
use std::{collections::HashMap, io};
pub type Name = Vec<Vec<u8>>;
#[derive(Clone, Default)]
pub struct Message {
    pub id: u16,
    pub flags: u16,
    pub questions: Vec<Question>,
    pub records: [Vec<Record>; 3],
}
#[derive(Clone)]
pub struct Question {
    pub name: Name,
    pub kind: u16,
    pub class: u16,
}
#[derive(Clone)]
pub struct Record {
    pub name: Name,
    pub kind: u16,
    pub class: u16,
    pub ttl: u32,
    pub data: Vec<u8>,
}
pub fn domain(text: &str) -> io::Result<Name> {
    let text = text.strip_suffix('.').unwrap_or(text);
    let name = if text.is_empty() {
        Vec::new()
    } else {
        text.as_bytes()
            .split(|b| *b == b'.')
            .map(<[u8]>::to_vec)
            .collect()
    };
    validate_name(&name)?;
    Ok(name)
}
pub fn validate_name(name: &Name) -> io::Result<()> {
    if name
        .iter()
        .any(|label| label.is_empty() || label.len() > 63)
        || name.iter().map(|v| v.len() + 1).sum::<usize>() + 1 > 255
    {
        return Err(invalid());
    }
    Ok(())
}
pub fn prefix<'a>(name: &'a Name, suffix: &Name) -> Option<&'a [Vec<u8>]> {
    let start = name.len().checked_sub(suffix.len())?;
    name[start..]
        .iter()
        .zip(suffix)
        .all(|(a, b)| a.eq_ignore_ascii_case(b))
        .then_some(&name[..start])
}
pub fn parse(bytes: &[u8]) -> io::Result<Message> {
    let mut p = Parser { bytes, at: 0 };
    let id = p.u16()?;
    let flags = p.u16()?;
    let counts = [p.u16()?, p.u16()?, p.u16()?, p.u16()?];
    if counts.iter().map(|n| usize::from(*n)).sum::<usize>() > 4096 {
        return Err(invalid());
    }
    let mut message = Message {
        id,
        flags,
        ..Default::default()
    };
    for _ in 0..counts[0] {
        message.questions.push(Question {
            name: p.name()?,
            kind: p.u16()?,
            class: p.u16()?,
        });
    }
    for (records, count) in message.records.iter_mut().zip(&counts[1..]) {
        for _ in 0..*count {
            let name = p.name()?;
            let kind = p.u16()?;
            let class = p.u16()?;
            let ttl = u32::from_be_bytes(p.take(4)?.try_into().unwrap());
            let length = usize::from(p.u16()?);
            records.push(Record {
                name,
                kind,
                class,
                ttl,
                data: p.take(length)?.to_vec(),
            });
        }
    }
    if p.at != bytes.len() {
        return Err(invalid());
    }
    Ok(message)
}
struct Parser<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Parser<'a> {
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self.at.checked_add(n).ok_or_else(invalid)?;
        let bytes = self.bytes.get(self.at..end).ok_or_else(invalid)?;
        self.at = end;
        Ok(bytes)
    }
    fn u16(&mut self) -> io::Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn name(&mut self) -> io::Result<Name> {
        let mut labels = Vec::new();
        let mut resume = None;
        let mut pointers = 0;
        loop {
            let length = self.take(1)?[0];
            match length & 0xc0 {
                0 => {
                    if length == 0 {
                        break;
                    }
                    labels.push(self.take(usize::from(length))?.to_vec());
                    if labels.len() > 127 {
                        return Err(invalid());
                    }
                }
                0xc0 => {
                    let offset = (usize::from(length & 0x3f) << 8) | usize::from(self.take(1)?[0]);
                    pointers += 1;
                    if pointers > 10 {
                        return Err(invalid());
                    }
                    resume.get_or_insert(self.at);
                    self.at = offset;
                }
                _ => return Err(invalid()),
            }
        }
        if let Some(at) = resume {
            self.at = at;
        }
        validate_name(&labels)?;
        Ok(labels)
    }
}
pub fn encode(message: &Message) -> io::Result<Vec<u8>> {
    let mut output = Builder {
        bytes: Vec::new(),
        names: HashMap::new(),
    };
    output.number(message.id);
    output.number(message.flags);
    for count in
        std::iter::once(message.questions.len()).chain(message.records.iter().map(Vec::len))
    {
        output.number(u16::try_from(count).map_err(|_| invalid())?);
    }
    for question in &message.questions {
        output.name(&question.name)?;
        output.number(question.kind);
        output.number(question.class);
    }
    for record in message.records.iter().flatten() {
        output.name(&record.name)?;
        output.number(record.kind);
        output.number(record.class);
        output.bytes.extend(record.ttl.to_be_bytes());
        output.number(u16::try_from(record.data.len()).map_err(|_| invalid())?);
        output.bytes.extend(&record.data);
    }
    if output.bytes.len() > 65535 {
        return Err(invalid());
    }
    Ok(output.bytes)
}
struct Builder {
    bytes: Vec<u8>,
    names: HashMap<Name, u16>,
}
impl Builder {
    fn number(&mut self, n: u16) {
        self.bytes.extend(n.to_be_bytes());
    }
    fn name(&mut self, name: &Name) -> io::Result<()> {
        validate_name(name)?;
        for (i, label) in name.iter().enumerate() {
            if let Some(offset) = self.names.get(&name[i..]) {
                self.number(0xc000 | *offset);
                return Ok(());
            }
            if self.bytes.len() < 16384 {
                self.names
                    .insert(name[i..].to_vec(), self.bytes.len() as u16);
            }
            self.bytes.push(label.len() as u8);
            self.bytes.extend(label);
        }
        self.bytes.push(0);
        Ok(())
    }
}
pub fn txt_encode(mut bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while bytes.len() > 255 {
        out.push(255);
        out.extend(&bytes[..255]);
        bytes = &bytes[255..];
    }
    out.push(bytes.len() as u8);
    out.extend(bytes);
    out
}
pub fn txt_decode(mut bytes: &[u8]) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let (&length, rest) = bytes.split_first().ok_or_else(invalid)?;
        let n = usize::from(length);
        out.extend(rest.get(..n).ok_or_else(invalid)?);
        bytes = &rest[n..];
        if bytes.is_empty() {
            return Ok(out);
        }
    }
}
pub fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid XDNS message")
}
