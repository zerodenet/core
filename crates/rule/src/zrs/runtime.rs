use std::fs::File;
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;

use fst::Set;
use memmap2::Mmap;

use crate::{PreparedRuleQuery, RuleMatch, RuleMatcher};

use super::layout::{read_u32, read_u64, Section};
use super::verify::{slice, verify_layout, Layout, RuleSetMetadata};
use super::{VerifyMode, ZrsError};

pub struct VerifiedRuleSet<'a> {
    bytes: &'a [u8],
    layout: Layout,
    exact: Set<&'a [u8]>,
    suffix: Set<&'a [u8]>,
}

impl<'a> VerifiedRuleSet<'a> {
    pub fn from_bytes(bytes: &'a [u8], mode: VerifyMode) -> Result<Self, ZrsError> {
        let layout = verify_layout(bytes, mode)?;
        let exact = Set::new(slice(bytes, layout.exact)).map_err(ZrsError::InvalidFst)?;
        let suffix = Set::new(slice(bytes, layout.suffix)).map_err(ZrsError::InvalidFst)?;
        Ok(Self {
            bytes,
            layout,
            exact,
            suffix,
        })
    }

    pub fn display_name(&self) -> Option<&str> {
        self.layout.metadata.display_name.as_deref()
    }

    pub fn metadata(&self) -> &RuleSetMetadata {
        &self.layout.metadata
    }

    pub fn matches(&self, query: &PreparedRuleQuery) -> bool {
        self.lookup(query).is_some()
    }

    pub fn lookup(&self, query: &PreparedRuleQuery) -> Option<RuleMatch> {
        lookup_indexes(
            &self.exact,
            &self.suffix,
            slice(self.bytes, self.layout.keyword),
            slice(self.bytes, self.layout.ipv4),
            slice(self.bytes, self.layout.ipv6),
            query,
        )
    }
}

impl RuleMatcher for VerifiedRuleSet<'_> {
    fn lookup(&self, query: &PreparedRuleQuery) -> Option<RuleMatch> {
        VerifiedRuleSet::lookup(self, query)
    }

    fn requires_destination_ip(&self) -> bool {
        read_u32(slice(self.bytes, self.layout.ipv4), 0) != 0
            || read_u32(slice(self.bytes, self.layout.ipv6), 0) != 0
    }
}

pub struct MappedRuleSet {
    mmap: Arc<Mmap>,
    roots: [(usize, usize); 5],
    metadata: RuleSetMetadata,
    exact: Set<MappedSection>,
    suffix: Set<MappedSection>,
    keyword: MappedSection,
    ipv4: MappedSection,
    ipv6: MappedSection,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PrewarmPolicy {
    #[default]
    Roots,
    FullFile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrewarmReport {
    pub touched_pages: usize,
    pub page_size: usize,
}

impl MappedRuleSet {
    /// Opens an immutable ZRS file.
    ///
    /// The backing file must never be truncated or overwritten in place while this value exists.
    /// Publishers must install updates through a new file and atomic replacement.
    pub fn open(path: impl AsRef<Path>, mode: VerifyMode) -> Result<Self, ZrsError> {
        let file = File::open(path)?;
        let file_size = file.metadata()?.len();
        if file_size > super::MAX_FILE_SIZE {
            return Err(ZrsError::ResourceLimit {
                resource: "file size",
                actual: file_size,
                maximum: super::MAX_FILE_SIZE,
            });
        }
        // SAFETY: this creates a read-only map whose ownership is retained by every section view.
        let mmap = Arc::new(unsafe { Mmap::map(&file)? });
        let layout = verify_layout(&mmap, mode)?;
        let exact = Set::new(MappedSection::new(Arc::clone(&mmap), layout.exact))
            .map_err(ZrsError::InvalidFst)?;
        let suffix = Set::new(MappedSection::new(Arc::clone(&mmap), layout.suffix))
            .map_err(ZrsError::InvalidFst)?;
        Ok(Self {
            mmap: Arc::clone(&mmap),
            roots: [
                section_endpoints(layout.exact),
                section_endpoints(layout.suffix),
                section_endpoints(layout.keyword),
                section_endpoints(layout.ipv4),
                section_endpoints(layout.ipv6),
            ],
            metadata: layout.metadata,
            exact,
            suffix,
            keyword: MappedSection::new(Arc::clone(&mmap), layout.keyword),
            ipv4: MappedSection::new(Arc::clone(&mmap), layout.ipv4),
            ipv6: MappedSection::new(mmap, layout.ipv6),
        })
    }

    pub fn display_name(&self) -> Option<&str> {
        self.metadata.display_name.as_deref()
    }

    pub fn metadata(&self) -> &RuleSetMetadata {
        &self.metadata
    }

    pub fn prewarm(&self, policy: PrewarmPolicy) -> PrewarmReport {
        const PAGE_SIZE: usize = 4096;
        let mmap = &self.mmap;
        let touched_pages = match policy {
            PrewarmPolicy::Roots => {
                let mut pages = Vec::with_capacity(11);
                pages.push(0);
                for (start, end) in self.roots {
                    pages.push(start / PAGE_SIZE);
                    pages.push(end / PAGE_SIZE);
                }
                pages.sort_unstable();
                let mut previous = None;
                let mut count = 0;
                for page in pages {
                    if previous == Some(page) {
                        continue;
                    }
                    std::hint::black_box(mmap[page * PAGE_SIZE]);
                    previous = Some(page);
                    count += 1;
                }
                count
            }
            PrewarmPolicy::FullFile => {
                let pages = mmap.len().div_ceil(PAGE_SIZE);
                for offset in (0..mmap.len()).step_by(PAGE_SIZE) {
                    std::hint::black_box(mmap[offset]);
                }
                pages
            }
        };
        PrewarmReport {
            touched_pages,
            page_size: PAGE_SIZE,
        }
    }

    pub fn matches(&self, query: &PreparedRuleQuery) -> bool {
        self.lookup(query).is_some()
    }

    pub fn lookup(&self, query: &PreparedRuleQuery) -> Option<RuleMatch> {
        lookup_indexes(
            &self.exact,
            &self.suffix,
            self.keyword.as_ref(),
            self.ipv4.as_ref(),
            self.ipv6.as_ref(),
            query,
        )
    }
}

fn section_endpoints(section: Section) -> (usize, usize) {
    (
        section.offset,
        section.offset + section.length.saturating_sub(1),
    )
}

impl RuleMatcher for MappedRuleSet {
    fn lookup(&self, query: &PreparedRuleQuery) -> Option<RuleMatch> {
        MappedRuleSet::lookup(self, query)
    }

    fn requires_destination_ip(&self) -> bool {
        read_u32(self.ipv4.as_ref(), 0) != 0 || read_u32(self.ipv6.as_ref(), 0) != 0
    }
}

#[derive(Clone)]
struct MappedSection {
    mmap: Arc<Mmap>,
    start: usize,
    end: usize,
}

impl MappedSection {
    fn new(mmap: Arc<Mmap>, section: Section) -> Self {
        Self {
            mmap,
            start: section.offset,
            end: section.offset + section.length,
        }
    }
}

impl AsRef<[u8]> for MappedSection {
    fn as_ref(&self) -> &[u8] {
        &self.mmap[self.start..self.end]
    }
}

fn lookup_indexes<D: AsRef<[u8]>>(
    exact: &Set<D>,
    suffix: &Set<D>,
    keyword: &[u8],
    ipv4: &[u8],
    ipv6: &[u8],
    query: &PreparedRuleQuery,
) -> Option<RuleMatch> {
    query
        .domain()
        .and_then(|domain| {
            if exact.contains(domain) {
                Some(RuleMatch::DomainExact)
            } else if suffix_matches(suffix, domain) {
                Some(RuleMatch::DomainSuffix)
            } else if keyword_matches(keyword, domain) {
                Some(RuleMatch::DomainKeyword)
            } else {
                None
            }
        })
        .or_else(|| {
            query.destination_ip().and_then(|address| match address {
                IpAddr::V4(address) => {
                    range4_matches(ipv4, u32::from(address)).then_some(RuleMatch::Ipv4Range)
                }
                IpAddr::V6(address) => {
                    range6_matches(ipv6, u128::from(address)).then_some(RuleMatch::Ipv6Range)
                }
            })
        })
}

fn suffix_matches<D: AsRef<[u8]>>(set: &Set<D>, domain: &str) -> bool {
    let mut candidate = domain;
    loop {
        if set.contains(candidate) {
            return true;
        }
        let Some(dot) = candidate.find('.') else {
            return false;
        };
        candidate = &candidate[dot + 1..];
    }
}

fn keyword_matches(bytes: &[u8], domain: &str) -> bool {
    let count = read_u32(bytes, 0) as usize;
    let data = 8 + (count + 1) * 8;
    (0..count).any(|index| {
        let start = read_u64(bytes, 8 + index * 8) as usize;
        let end = read_u64(bytes, 16 + index * 8) as usize;
        domain.contains(
            std::str::from_utf8(&bytes[data + start..data + end]).expect("verified UTF-8"),
        )
    })
}

fn range4_matches(bytes: &[u8], value: u32) -> bool {
    let count = read_u32(bytes, 0) as usize;
    let index = predecessor(count, |index| read_u32(bytes, 8 + index * 8) <= value);
    index > 0 && value <= read_u32(bytes, 12 + (index - 1) * 8)
}

fn range6_matches(bytes: &[u8], value: u128) -> bool {
    let count = read_u32(bytes, 0) as usize;
    let read = |offset| {
        u128::from_le_bytes(
            bytes[offset..offset + 16]
                .try_into()
                .expect("verified bounds"),
        )
    };
    let index = predecessor(count, |index| read(8 + index * 32) <= value);
    index > 0 && value <= read(24 + (index - 1) * 32)
}

fn predecessor(count: usize, predicate: impl Fn(usize) -> bool) -> usize {
    let (mut left, mut right) = (0, count);
    while left < right {
        let middle = left + (right - left) / 2;
        if predicate(middle) {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    left
}
