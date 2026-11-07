//! Allocation-free sizing for the one concrete owned initial JSON envelope.
//! Map storage uses the pinned std B-tree's B=6 node envelope, also conservative
//! for serde_json's optional ordered map. This is a reservation bound, not RSS.
use serde::{ser::*, Serialize, Serializer};
use std::{
    fmt::{self, Write},
    mem::size_of,
};
type Error = serde_json::Error;
type Result<T> = std::result::Result<T, Error>;
fn overflow() -> Error {
    <Error as serde::ser::Error>::custom("initial snapshot storage overflow")
}
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or_else(overflow)
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or_else(overflow)
}
fn map_storage(entries: usize) -> Result<usize> {
    if entries == 0 {
        return Ok(0);
    }
    // Nonroot B-tree nodes have at least five keys. Each actual key is owned
    // separately below. One maximal node per possible actual node is prepaid.
    let node = add(
        mul(15, size_of::<usize>())?,
        mul(11, size_of::<String>() + size_of::<serde_json::Value>())?,
    )?;
    mul(1 + (entries - 1) / 5, node)
}
pub(super) fn heap(value: &impl Serialize) -> Result<usize> {
    value.serialize(Meter).map(|v| v.heap)
}
pub(super) fn display_len(value: impl fmt::Display) -> Result<usize> {
    struct Count(usize);
    impl Write for Count {
        fn write_str(&mut self, s: &str) -> fmt::Result {
            self.0 = self.0.checked_add(s.len()).ok_or(fmt::Error)?;
            Ok(())
        }
    }
    let mut count = Count(0);
    write!(&mut count, "{value}").map_err(|_| overflow())?;
    Ok(count.0)
}
pub(super) struct Size {
    heap: usize,
    key: Option<usize>,
}
struct Meter;
impl Size {
    fn scalar() -> Self {
        Self { heap: 0, key: None }
    }
    fn text(bytes: usize) -> Self {
        Self {
            heap: bytes,
            key: Some(bytes),
        }
    }
    fn number(value: impl fmt::Display) -> Result<Self> {
        Ok(Self {
            heap: 0,
            key: Some(display_len(value)?),
        })
    }
}
struct Compound {
    count: usize,
    hint: usize,
    heap: usize,
    map: bool,
    variant: usize,
}
impl Compound {
    fn new(hint: Option<usize>, map: bool, variant: usize) -> Self {
        Self {
            count: 0,
            hint: hint.unwrap_or(0),
            heap: 0,
            map,
            variant,
        }
    }
    fn value(&mut self, value: &impl Serialize) -> Result<()> {
        self.heap = add(self.heap, value.serialize(Meter)?.heap)?;
        self.count = add(self.count, 1)?;
        Ok(())
    }
    fn key(&mut self, key: &impl Serialize) -> Result<()> {
        let bytes = key.serialize(Meter)?.key.ok_or_else(|| {
            <Error as serde::ser::Error>::custom("initial JSON object key is not a string")
        })?;
        self.heap = add(self.heap, bytes)?;
        Ok(())
    }
    fn finish(self) -> Result<Size> {
        let storage = if self.map {
            map_storage(self.count)?
        } else {
            // Value's sequence serializer preserves its length hint; unknown
            // lengths use actual Vec amortized growth, at most max(4,2*len).
            let cap = if self.count <= self.hint {
                self.hint
            } else {
                mul(self.count, 2)?.max(4)
            };
            mul(cap, size_of::<serde_json::Value>())?
        };
        Ok(Size {
            heap: add(add(self.heap, storage)?, self.variant)?,
            key: None,
        })
    }
}
macro_rules! number {($($method:ident:$ty:ty),*)=>{$(fn $method(self,value:$ty)->Result<Size>{Size::number(value)})*};}
impl Serializer for Meter {
    type Ok = Size;
    type Error = Error;
    type SerializeSeq = Compound;
    type SerializeTuple = Compound;
    type SerializeTupleStruct = Compound;
    type SerializeTupleVariant = Compound;
    type SerializeMap = Compound;
    type SerializeStruct = Compound;
    type SerializeStructVariant = Compound;
    fn serialize_bool(self, v: bool) -> Result<Size> {
        Size::number(v)
    }
    number!(serialize_i8:i8,serialize_i16:i16,serialize_i32:i32,serialize_i64:i64,serialize_i128:i128,serialize_u8:u8,serialize_u16:u16,serialize_u32:u32,serialize_u64:u64,serialize_u128:u128,serialize_f32:f32,serialize_f64:f64);
    fn serialize_char(self, v: char) -> Result<Size> {
        Ok(Size::text(v.len_utf8()))
    }
    fn serialize_str(self, v: &str) -> Result<Size> {
        Ok(Size::text(v.len()))
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<Size> {
        Ok(Size {
            heap: mul(v.len(), size_of::<serde_json::Value>())?,
            key: None,
        })
    }
    fn serialize_none(self) -> Result<Size> {
        Ok(Size::scalar())
    }
    fn serialize_some<T: Serialize + ?Sized>(self, v: &T) -> Result<Size> {
        v.serialize(self)
    }
    fn serialize_unit(self) -> Result<Size> {
        Ok(Size::scalar())
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<Size> {
        self.serialize_unit()
    }
    fn serialize_unit_variant(self, _: &'static str, _: u32, v: &'static str) -> Result<Size> {
        self.serialize_str(v)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        v: &T,
    ) -> Result<Size> {
        v.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        name: &'static str,
        v: &T,
    ) -> Result<Size> {
        let heap = add(add(map_storage(1)?, name.len())?, v.serialize(self)?.heap)?;
        Ok(Size { heap, key: None })
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Compound> {
        Ok(Compound::new(len, false, 0))
    }
    fn serialize_tuple(self, len: usize) -> Result<Compound> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_struct(self, _: &'static str, len: usize) -> Result<Compound> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        name: &'static str,
        len: usize,
    ) -> Result<Compound> {
        Ok(Compound::new(
            Some(len),
            false,
            add(map_storage(1)?, name.len())?,
        ))
    }
    fn serialize_map(self, len: Option<usize>) -> Result<Compound> {
        Ok(Compound::new(len, true, 0))
    }
    fn serialize_struct(self, _: &'static str, len: usize) -> Result<Compound> {
        self.serialize_map(Some(len))
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        name: &'static str,
        len: usize,
    ) -> Result<Compound> {
        Ok(Compound::new(
            Some(len),
            true,
            add(map_storage(1)?, name.len())?,
        ))
    }
    fn collect_str<T: fmt::Display + ?Sized>(self, value: &T) -> Result<Size> {
        Ok(Size::text(add(mul(display_len(value)?, 2)?, 8)?))
    }
}
impl SerializeSeq for Compound {
    type Ok = Size;
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<()> {
        self.value(&v)
    }
    fn end(self) -> Result<Size> {
        self.finish()
    }
}
impl SerializeTuple for Compound {
    type Ok = Size;
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<()> {
        self.value(&v)
    }
    fn end(self) -> Result<Size> {
        self.finish()
    }
}
impl SerializeTupleStruct for Compound {
    type Ok = Size;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<()> {
        self.value(&v)
    }
    fn end(self) -> Result<Size> {
        self.finish()
    }
}
impl SerializeTupleVariant for Compound {
    type Ok = Size;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<()> {
        self.value(&v)
    }
    fn end(self) -> Result<Size> {
        self.finish()
    }
}
impl SerializeMap for Compound {
    type Ok = Size;
    type Error = Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<()> {
        self.key(&key)
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<()> {
        self.value(&v)
    }
    fn end(self) -> Result<Size> {
        self.finish()
    }
}
impl SerializeStruct for Compound {
    type Ok = Size;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, key: &'static str, v: &T) -> Result<()> {
        self.key(&key)?;
        self.value(&v)
    }
    fn end(self) -> Result<Size> {
        self.finish()
    }
}
impl SerializeStructVariant for Compound {
    type Ok = Size;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, key: &'static str, v: &T) -> Result<()> {
        self.key(&key)?;
        self.value(&v)
    }
    fn end(self) -> Result<Size> {
        self.finish()
    }
}
