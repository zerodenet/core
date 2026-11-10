//! Concrete quota map/string and final-flush storage in the same Source book.
use super::*;
use std::ops::Deref;
use zero_runtime::configuration::{QuotaMemoryBudget, QuotaMemoryCredit};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataLayout, PacketMetadataWeak};

#[derive(Clone)]
pub(super) enum BookOwner {
    // Only the existing internal genuinely standalone Default constructor.
    Library(Arc<Mutex<QuotaBook>>),
    Paid(PacketMetadataArc<Mutex<QuotaBook>>),
}
pub(super) enum WeakBook {
    Library(std::sync::Weak<Mutex<QuotaBook>>),
    Paid(PacketMetadataWeak<Mutex<QuotaBook>>),
}
impl std::fmt::Debug for BookOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QuotaBookOwner")
    }
}
impl Deref for BookOwner {
    type Target = Mutex<QuotaBook>;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Library(value) => value,
            Self::Paid(value) => value,
        }
    }
}
impl BookOwner {
    pub(super) fn paid(book: QuotaBook, memory: &QuotaMemoryBudget) -> io::Result<Self> {
        let credit = memory
            .reserve(PacketMetadataArc::<Mutex<QuotaBook>>::layout().map_err(io::Error::other)?)?;
        match PacketMetadataArc::from_prepaid(Mutex::new(book), credit.into_metadata()) {
            Ok(value) => Ok(Self::Paid(value)),
            Err((error, value, credit)) => {
                drop(value);
                drop(credit);
                Err(io::Error::other(error))
            }
        }
    }
    pub(super) fn downgrade(&self) -> WeakBook {
        match self {
            Self::Library(value) => WeakBook::Library(Arc::downgrade(value)),
            Self::Paid(value) => WeakBook::Paid(PacketMetadataArc::downgrade(value)),
        }
    }
}
impl WeakBook {
    pub(super) fn upgrade(&self) -> Option<BookOwner> {
        match self {
            Self::Library(value) => value.upgrade().map(BookOwner::Library),
            Self::Paid(value) => value.upgrade().map(BookOwner::Paid),
        }
    }
}
#[derive(Debug)]
pub(super) struct Current {
    pub(super) key: QuotaKey,
    // Map key String is destroyed before this value's key and credit. Quota
    // grants issue only a sealed noop Waker; no callback executes under book.
    pub(super) promise: PacketMetadataLayout,
    pub(super) _strings: Option<QuotaMemoryCredit>,
}
pub(super) fn key_storage(length: usize, copies: usize) -> io::Result<PacketMetadataLayout> {
    let bytes = length.checked_mul(copies).ok_or_else(overflow)?;
    Ok(PacketMetadataLayout::new(
        if length == 0 { 0 } else { copies },
        bytes,
    ))
}
fn overflow() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "quota storage layout overflow")
}
fn empty() -> PacketMetadataLayout {
    PacketMetadataLayout::new(0, 0)
}
fn subtract(
    left: PacketMetadataLayout,
    right: PacketMetadataLayout,
) -> io::Result<PacketMetadataLayout> {
    Ok(PacketMetadataLayout::new(
        left.allocations()
            .checked_sub(right.allocations())
            .ok_or_else(overflow)?,
        left.bytes()
            .checked_sub(right.bytes())
            .ok_or_else(overflow)?,
    ))
}
fn maximum(left: PacketMetadataLayout, right: PacketMetadataLayout) -> PacketMetadataLayout {
    PacketMetadataLayout::new(
        left.allocations().max(right.allocations()),
        left.bytes().max(right.bytes()),
    )
}
/// HashMap retains its real bucket allocation across removals. Capacity of
/// live rows alone cannot refund that allocation; keep one ticket per actual
/// map, replacing it only after try_reserve has really freed the old buckets.
fn map_layout<K, V>(entries: usize) -> io::Result<PacketMetadataLayout> {
    if entries == 0 {
        return Ok(empty());
    }
    // Hashbrown load factor <=7/8; minimum four buckets. Include control group
    // bytes and alignment rather than charging just the entry pointers.
    let buckets = entries
        .checked_mul(8)
        .and_then(|n| n.checked_add(6))
        .map(|n| n / 7)
        .ok_or_else(overflow)?;
    let buckets = buckets
        .max(4)
        .checked_next_power_of_two()
        .ok_or_else(overflow)?;
    let row = std::mem::size_of::<(K, V)>()
        .checked_add(1)
        .ok_or_else(overflow)?;
    let bytes = buckets
        .checked_mul(row)
        .and_then(|n| n.checked_add(32 + std::mem::align_of::<(K, V)>()))
        .ok_or_else(overflow)?;
    Ok(PacketMetadataLayout::new(1, bytes))
}
#[must_use]
pub(super) struct MapReleased {
    _old: Option<QuotaMemoryCredit>,
    _extra: Option<QuotaMemoryCredit>,
}
pub(super) fn grow<K: std::hash::Hash + Eq, V>(
    map: &mut HashMap<K, V>,
    ticket: &mut Option<QuotaMemoryCredit>,
    memory: Option<&QuotaMemoryBudget>,
    additional: usize,
) -> io::Result<MapReleased> {
    let needed = map.len().checked_add(additional).ok_or_else(overflow)?;
    if needed <= map.capacity() {
        return Ok(MapReleased {
            _old: None,
            _extra: None,
        });
    }
    let measured = map_layout::<K, V>(needed)?;
    // Tombstones can lower capacity() while retaining the old physical table.
    // Hashbrown may rehash in place or double it; prepay both possible new
    // shapes while the old table's distinct ticket remains live. After actual
    // reserve, the table has no old tombstones and its real capacity measures
    // the retained new allocation. Unused promise leaves the caller's guard.
    let prior = ticket.as_ref().map_or(empty(), QuotaMemoryCredit::layout);
    let worst = maximum(
        measured,
        PacketMetadataLayout::new(
            prior.allocations(),
            prior.bytes().checked_mul(2).ok_or_else(overflow)?,
        ),
    );
    let mut new_ticket = memory.map(|memory| memory.reserve(worst)).transpose()?;
    map.try_reserve(additional).map_err(io::Error::other)?;
    let actual = map_layout::<K, V>(map.capacity())?;
    let extra = match new_ticket.as_mut() {
        Some(credit) => {
            let excess = subtract(credit.layout(), actual)?;
            if excess == empty() {
                None
            } else {
                Some(credit.split(excess)?)
            }
        }
        None => None,
    };
    Ok(MapReleased {
        _old: std::mem::replace(ticket, new_ticket),
        _extra: extra,
    })
}
pub(super) fn snapshot_layout(key: &QuotaKey) -> io::Result<PacketMetadataLayout> {
    let encoded = super::snapshot::encoded_row_bound(key)?;
    PacketMetadataLayout::array::<PersistedQuotaBalance>(1)
        .and_then(|row| row.checked_add(PacketMetadataLayout::array::<u8>(key.principal_key.len())?))
        .and_then(|row| row.checked_add(PacketMetadataLayout::array::<u8>(encoded)?))
        .map_err(io::Error::other)
}

#[derive(Debug)]
pub(super) struct Promise {
    desired: PacketMetadataLayout,
    working: PacketMetadataLayout,
    credit: Option<QuotaMemoryCredit>,
}
impl Default for Promise {
    fn default() -> Self {
        Self {
            desired: empty(),
            working: empty(),
            credit: None,
        }
    }
}
impl Promise {
    pub(super) fn prepare_delta(
        &self,
        memory: Option<&QuotaMemoryBudget>,
        added: PacketMetadataLayout,
    ) -> io::Result<Option<QuotaMemoryCredit>> {
        let desired = self.desired.checked_add(added).map_err(io::Error::other)?;
        let have = self.credit.as_ref().map_or(self.working, |credit| {
            self.working
                .checked_add(credit.layout())
                .expect("same quota promise")
        });
        let target = maximum(desired, have);
        let delta = subtract(target, have)?;
        if delta == empty() {
            return Ok(None);
        }
        memory.map(|memory| memory.reserve(delta)).transpose()
    }
    pub(super) fn add(
        &mut self,
        layout: PacketMetadataLayout,
        credit: Option<QuotaMemoryCredit>,
    ) -> Option<QuotaMemoryCredit> {
        self.desired = self
            .desired
            .checked_add(layout)
            .expect("prepaid quota promise delta");
        self.merge(credit)
    }
    fn merge(&mut self, credit: Option<QuotaMemoryCredit>) -> Option<QuotaMemoryCredit> {
        match (self.credit.as_mut(), credit) {
            (Some(current), Some(other)) => match current.absorb(other) {
                Ok(empty) => Some(empty),
                Err(_) => unreachable!("same quota counter and prechecked total"),
            },
            (None, other) => {
                self.credit = other;
                None
            }
            (_, None) => None,
        }
    }
    pub(super) fn remove(&mut self, layout: PacketMetadataLayout) {
        self.desired = subtract(self.desired, layout).expect("actual quota current row promise");
    }
    pub(super) fn trim(&mut self) -> Option<QuotaMemoryCredit> {
        let desired = maximum(self.desired, self.working);
        let held = self
            .credit
            .as_ref()?
            .layout()
            .checked_add(self.working)
            .expect("same quota promise");
        let extra = subtract(held, desired).ok()?;
        if extra == empty() {
            return None;
        }
        Some(
            self.credit
                .as_mut()
                .unwrap()
                .split(extra)
                .expect("actual unneeded quota promise"),
        )
    }
}
/// Holds only a Weak book and already-paid memory, never an IO/Scope owner.
/// Caller's frozen rows/encoded bytes must be declared after this guard so
/// they are actually destroyed before any custody can return to the book.
pub(super) struct SnapshotCustody {
    book: WeakBook,
    credit: Option<QuotaMemoryCredit>,
}
impl SnapshotCustody {
    pub(super) fn take_locked(book: &BookOwner, locked: &mut QuotaBook) -> io::Result<Self> {
        if locked.promise.working != empty() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "quota snapshot already working",
            ));
        }
        let credit = locked.promise.credit.take();
        locked.promise.working = credit.as_ref().map_or(empty(), QuotaMemoryCredit::layout);
        Ok(Self {
            book: book.downgrade(),
            credit,
        })
    }
}
impl Drop for SnapshotCustody {
    fn drop(&mut self) {
        let Some(book) = self.book.upgrade() else {
            return;
        };
        let (empty, unused) = {
            let mut locked = book.lock().unwrap_or_else(|error| error.into_inner());
            locked.promise.working = empty();
            let empty = locked.promise.merge(self.credit.take());
            let unused = locked.promise.trim();
            (empty, unused)
        };
        drop(empty);
        drop(unused);
    }
}

pub(super) fn new_book(memory: &QuotaMemoryBudget, persistent: bool) -> io::Result<QuotaBook> {
    // Fixed serialization envelope for version=1 and the balances array. The
    // per-current-row promise accounts every additional byte/string/Vec row.
    let mut book = QuotaBook {
        memory: Some(memory.clone()),
        persistent,
        ..Default::default()
    };
    if persistent {
        let base = PacketMetadataLayout::array::<u8>(64).map_err(io::Error::other)?;
        let credit = memory.reserve(base)?;
        drop(book.promise.add(base, Some(credit)));
    }
    Ok(book)
}
