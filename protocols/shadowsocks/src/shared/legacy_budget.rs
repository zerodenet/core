//! One adapter's actual fixed legacy Bloom bitmap allocations, not RSS or salts.
use super::legacy_replay::Book;
use std::sync::{Arc, Mutex, Weak};
use zero_core::Error;

#[derive(Debug, Default)]
struct State {
    limit: Option<usize>,
    retained: usize,
}
pub struct LegacyReplayBudget {
    state: Mutex<State>,
    // Retain the original membership filters, never a copied history. There is
    // no legacy timestamp expiry that would permit pressure-based eviction.
    books: Mutex<Vec<Arc<Mutex<Book>>>>,
}
impl Default for LegacyReplayBudget {
    fn default() -> Self {
        Self {
            state: Default::default(),
            books: Default::default(),
        }
    }
}
impl std::fmt::Debug for LegacyReplayBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LegacyReplayBudget")
            .field("bitmap_bytes", &self.retained())
            .finish_non_exhaustive()
    }
}
impl LegacyReplayBudget {
    /// Allocates no bitmap. Production supplies its explicit finite policy;
    /// genuine standalone callers can retain the original unbounded default.
    pub fn new(max_bytes: Option<usize>) -> Result<Arc<Self>, Error> {
        validate_limit(max_bytes)?;
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                limit: max_bytes,
                retained: 0,
            }),
            books: Default::default(),
        }))
    }
    pub fn retained(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retained
    }
    pub fn validate(&self, max_bytes: Option<usize>) -> Result<(), Error> {
        validate_limit(max_bytes)?;
        if max_bytes.is_some_and(|limit| self.retained() > limit) {
            return Err(Error::Config(
                "retained legacy Bloom bitmaps exceed the proposed byte budget",
            ));
        }
        Ok(())
    }
    pub(crate) fn install(&self, max_bytes: usize) -> Result<(), Error> {
        validate_limit(Some(max_bytes))?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.retained > max_bytes {
            return Err(Error::Config(
                "legacy Bloom bitmap debt grew before replacement admission paused",
            ));
        }
        state.limit = Some(max_bytes);
        Ok(())
    }
    pub(super) fn claim(
        self: &Arc<Self>,
        original: &Arc<Mutex<Book>>,
        server: bool,
    ) -> Result<Charge, Error> {
        let bytes = bitmap_bytes(server);
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let next = state
                .retained
                .checked_add(bytes)
                .ok_or(Error::Protocol("legacy Bloom byte count overflow"))?;
            if state.limit.is_some_and(|limit| next > limit) {
                return Err(Error::Protocol("legacy Bloom bitmap byte budget exhausted"));
            }
            state.retained = next;
        }
        let charge = Charge {
            budget: Arc::downgrade(self),
            bytes,
        };
        let mut books = self
            .books
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Empty failed preparations have no membership to preserve. Active or
        // valid originals are never discarded, regardless of pressure.
        books.retain(|book| {
            Arc::strong_count(book) != 1 || book.try_lock().map_or(true, |book| book.has_history())
        });
        if !books.iter().any(|book| Arc::ptr_eq(book, original)) {
            books.push(original.clone());
        }
        Ok(charge)
    }
}
fn validate_limit(limit: Option<usize>) -> Result<(), Error> {
    if limit.is_some_and(|limit| limit == 0 || limit.checked_add(1).is_none()) {
        return Err(Error::Config(
            "legacy Bloom bitmap byte budget must be positive and countable",
        ));
    }
    Ok(())
}
/// bloomfilter1.0.14 allocates BitVec<u32>. Count both fixed reference filters,
/// rounded to actual u32 payload blocks; allocator metadata is not claimed RSS.
pub(crate) fn bitmap_bytes(server: bool) -> usize {
    let (items, probability) = if server {
        (500_000, 1e-6)
    } else {
        (5_000, 1e-15)
    };
    let bytes = bloomfilter::Bloom::<[u8]>::compute_bitmap_size(items, probability);
    let block = std::mem::size_of::<u32>();
    bytes
        .div_ceil(block)
        .checked_mul(block)
        .and_then(|bytes| bytes.checked_mul(2))
        .expect("fixed reference Bloom bitmap size")
}
pub(super) struct Charge {
    budget: Weak<LegacyReplayBudget>,
    bytes: usize,
}
impl Drop for Charge {
    fn drop(&mut self) {
        if let Some(budget) = self.budget.upgrade() {
            let mut state = budget
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.retained = state
                .retained
                .checked_sub(self.bytes)
                .expect("original legacy Bloom byte charge");
        }
    }
}
#[derive(Clone, Debug)]
pub struct LegacyReplayBudgetProposal {
    original: Weak<LegacyReplayBudget>,
    max_bytes: usize,
}
impl LegacyReplayBudgetProposal {
    pub(crate) fn new(original: &Arc<LegacyReplayBudget>, max_bytes: usize) -> Result<Self, Error> {
        original.validate(Some(max_bytes))?;
        Ok(Self {
            original: Arc::downgrade(original),
            max_bytes,
        })
    }
    pub(crate) fn identity(&self) -> (usize, usize) {
        (self.original.as_ptr() as usize, self.max_bytes)
    }
    pub(crate) fn activate(&self) -> Result<(), Error> {
        self.original
            .upgrade()
            .ok_or(Error::Protocol("legacy Bloom budget owner was released"))?
            .install(self.max_bytes)
    }
}

#[cfg(test)]
#[path = "../../tests/legacy_budget.rs"]
mod tests;
