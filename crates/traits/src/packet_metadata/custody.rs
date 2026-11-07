//! Only allocation custody. All Arc/Weak pointers stay private, so a surviving
//! weak header retains its exact prepaid layout. This grants no IO or policy.
use super::*;
use alloc::sync::Weak;
use core::ops::Deref;

pub struct PacketMetadataArc<T> {
    allocation: Option<Arc<T>>,
    credit: Option<Arc<PacketMetadataCredit>>,
}
pub struct PacketMetadataWeak<T> {
    allocation: Option<Weak<T>>,
    credit: Option<Arc<PacketMetadataCredit>>,
}
impl<T> PacketMetadataArc<T> {
    /// Both allocations must be paid before construction of their owning
    /// object. This includes the attachment's own real Arc header/layout.
    pub fn layout() -> Result<PacketMetadataLayout, PacketMemoryError> {
        PacketMetadataLayout::arc::<T>()?
            .checked_add(PacketMetadataLayout::arc::<PacketMetadataCredit>()?)
    }
    pub fn from_prepaid(
        value: T,
        credit: PacketMetadataCredit,
    ) -> Result<Self, (PacketMemoryError, T, PacketMetadataCredit)> {
        let expected = match Self::layout() {
            Ok(expected) => expected,
            Err(source) => return Err((source, value, credit)),
        };
        let actual = credit.layout();
        if actual.allocations() < expected.allocations() || actual.bytes() < expected.bytes() {
            return Err((PacketMemoryError::Capacity, value, credit));
        }
        Ok(Self {
            allocation: Some(Arc::new(value)),
            credit: Some(Arc::new(credit)),
        })
    }
    pub fn downgrade(this: &Self) -> PacketMetadataWeak<T> {
        PacketMetadataWeak {
            allocation: Some(Arc::downgrade(this.allocation.as_ref().unwrap())),
            credit: this.credit.clone(),
        }
    }
    pub fn ptr_eq(left: &Self, right: &Self) -> bool {
        Arc::ptr_eq(
            left.allocation.as_ref().unwrap(),
            right.allocation.as_ref().unwrap(),
        )
    }
    pub fn get_mut(this: &mut Self) -> Option<&mut T> {
        Arc::get_mut(this.allocation.as_mut().unwrap())
    }
}
impl<T> Clone for PacketMetadataArc<T> {
    fn clone(&self) -> Self {
        Self {
            allocation: self.allocation.clone(),
            credit: self.credit.clone(),
        }
    }
}
impl<T> Deref for PacketMetadataArc<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.allocation.as_ref().unwrap()
    }
}
impl<T> AsRef<T> for PacketMetadataArc<T> {
    fn as_ref(&self) -> &T {
        self
    }
}
impl<T> PacketMetadataWeak<T> {
    pub fn upgrade(&self) -> Option<PacketMetadataArc<T>> {
        Some(PacketMetadataArc {
            allocation: Some(self.allocation.as_ref().unwrap().upgrade()?),
            credit: self.credit.clone(),
        })
    }
    pub fn ptr_eq(left: &Self, right: &Self) -> bool {
        Weak::ptr_eq(
            left.allocation.as_ref().unwrap(),
            right.allocation.as_ref().unwrap(),
        )
    }
}
impl<T> Clone for PacketMetadataWeak<T> {
    fn clone(&self) -> Self {
        Self {
            allocation: self.allocation.clone(),
            credit: self.credit.clone(),
        }
    }
}
fn release<T>(allocation: Option<T>, credit: Option<Arc<PacketMetadataCredit>>) {
    // This handle drops its actual strong/weak pointer FIRST. Every handle
    // carries the attachment, so the final one cannot refund a live header.
    struct CreditRelease(Option<Arc<PacketMetadataCredit>>);
    impl Drop for CreditRelease {
        fn drop(&mut self) {
            if let Some(credit) = self.0.take() {
                // into_inner has exactly one winner for concurrent custodians.
                // Its private Arc allocation is freed before credit returns.
                // This guard also runs if the actual payload destructor panics.
                drop(Arc::into_inner(credit));
            }
        }
    }
    let credit = CreditRelease(credit);
    drop(allocation);
    drop(credit);
}
impl<T> Drop for PacketMetadataArc<T> {
    fn drop(&mut self) {
        release(self.allocation.take(), self.credit.take());
    }
}
impl<T> Drop for PacketMetadataWeak<T> {
    fn drop(&mut self) {
        release(self.allocation.take(), self.credit.take());
    }
}
