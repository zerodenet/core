#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Range {
    pub start: u64,
    pub length: u64,
}
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Error {
    Invalid,
    NoOverlap,
}
impl Error {
    pub fn message(&self) -> &'static [u8] {
        match self {
            Self::Invalid => b"invalid range\n",
            Self::NoOverlap => b"invalid range: failed to overlap\n",
        }
    }
}
fn number(value: &str) -> Result<u64, Error> {
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value >= 0)
        .map(|value| value as u64)
        .ok_or(Error::Invalid)
}
pub(super) fn parse(value: Option<&str>, size: u64) -> Result<Vec<Range>, Error> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Ok(Vec::new());
    };
    let value = value.strip_prefix("bytes=").ok_or(Error::Invalid)?;
    let mut ranges = Vec::new();
    let mut no_overlap = false;
    for item in value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let (start, end) = item.split_once('-').ok_or(Error::Invalid)?;
        let (start, end) = (start.trim(), end.trim());
        if start.is_empty() {
            let length = number(end)?.min(size);
            ranges.push(Range {
                start: size - length,
                length,
            });
        } else {
            let start = number(start)?;
            if start >= size {
                no_overlap = true;
                continue;
            }
            let end = if end.is_empty() {
                size - 1
            } else {
                number(end)?.min(size - 1)
            };
            if end < start {
                return Err(Error::Invalid);
            }
            ranges.push(Range {
                start,
                length: end - start + 1,
            });
        }
    }
    if no_overlap && ranges.is_empty() && size != 0 {
        return Err(Error::NoOverlap);
    }
    if ranges
        .iter()
        .try_fold(0u64, |total, range| total.checked_add(range.length))
        .is_none_or(|total| total > size)
    {
        ranges.clear();
    }
    Ok(ranges)
}
impl Range {
    pub fn content_range(self, size: u64) -> String {
        format!(
            "bytes {}-{}/{size}",
            self.start,
            self.start as i128 + self.length as i128 - 1
        )
    }
    pub fn prefix(self, size: u64, mime: &str, boundary: &str, first: bool) -> String {
        format!(
            "{}--{boundary}\r\nContent-Range: {}\r\nContent-Type: {mime}\r\n\r\n",
            if first { "" } else { "\r\n" },
            self.content_range(size)
        )
    }
}
