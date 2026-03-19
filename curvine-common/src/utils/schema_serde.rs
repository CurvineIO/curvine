// curvine-common/src/utils/schema_serde.rs

use serde::ser::{self, Serialize};
use std::fmt;

// ============================================================================
// Error type
// ============================================================================

#[derive(Debug)]
pub struct SchemaSerdeError(String);

impl fmt::Display for SchemaSerdeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SchemaSerdeError: {}", self.0)
    }
}

impl std::error::Error for SchemaSerdeError {}

impl ser::Error for SchemaSerdeError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        SchemaSerdeError(msg.to_string())
    }
}

// ============================================================================
// TypeTag — identifies each type in the schema
// ============================================================================

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TypeTag {
    Bool = 0,
    I8 = 1,
    I16 = 2,
    I32 = 3,
    I64 = 4,
    U8 = 5,
    U16 = 6,
    U32 = 7,
    U64 = 8,
    F32 = 9,
    F64 = 10,
    Char = 11,
    String = 12,
    Bytes = 13,
    None = 14,   // Option::None
    Some = 15,   // Option::Some — followed by inner schema
    Seq = 16,    // followed by element count (u32) + per-element schema
    Map = 17,    // followed by entry count (u32) + per-entry key/value schema
    Struct = 18, // followed by field_count (u16) + field schemas
    Enum = 19,   // followed by variant_index (u32) + variant_name + inner schema
    Unit = 20,
    I128 = 21,
    U128 = 22,
    Tuple = 23, // followed by element count (u16) + element schemas
}

impl TypeTag {
    pub fn from_u8(v: u8) -> Result<Self, SchemaSerdeError> {
        match v {
            0 => Ok(Self::Bool),
            1 => Ok(Self::I8),
            2 => Ok(Self::I16),
            3 => Ok(Self::I32),
            4 => Ok(Self::I64),
            5 => Ok(Self::U8),
            6 => Ok(Self::U16),
            7 => Ok(Self::U32),
            8 => Ok(Self::U64),
            9 => Ok(Self::F32),
            10 => Ok(Self::F64),
            11 => Ok(Self::Char),
            12 => Ok(Self::String),
            13 => Ok(Self::Bytes),
            14 => Ok(Self::None),
            15 => Ok(Self::Some),
            16 => Ok(Self::Seq),
            17 => Ok(Self::Map),
            18 => Ok(Self::Struct),
            19 => Ok(Self::Enum),
            20 => Ok(Self::Unit),
            21 => Ok(Self::I128),
            22 => Ok(Self::U128),
            23 => Ok(Self::Tuple),
            _ => Err(SchemaSerdeError(format!("unknown type tag: {}", v))),
        }
    }
}

// ============================================================================
// Schema & Value writers — internal buffers for the serializer
// ============================================================================

/// Accumulates schema bytes during serialization.  
struct SchemaWriter {
    buf: Vec<u8>,
}

impl SchemaWriter {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(256),
        }
    }

    fn write_tag(&mut self, tag: TypeTag) {
        self.buf.push(tag as u8);
    }

    /// Writes a field name: [name_len: u16][name_bytes...]  
    fn write_field_name(&mut self, name: &str) {
        let len = name.len() as u16;
        self.buf.extend_from_slice(&len.to_le_bytes());
        self.buf.extend_from_slice(name.as_bytes());
    }

    /// Writes a u16 count (used for struct field count, tuple element count)  
    fn write_u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// Writes a u32 count (used for seq length, map length, enum variant index)  
    fn write_u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    fn into_bytes(self) -> Vec<u8> {
        self.buf
    }
}

/// Accumulates value bytes during serialization.
struct ValueWriter {
    buf: Vec<u8>,
}

impl ValueWriter {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(512),
        }
    }

    fn write_bool(&mut self, v: bool) {
        self.buf.push(if v { 1 } else { 0 });
    }

    fn write_i8(&mut self, v: i8) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_i16(&mut self, v: i16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_i128(&mut self, v: i128) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_u8(&mut self, v: u8) {
        self.buf.push(v);
    }
    fn write_u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_u128(&mut self, v: u128) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_f32(&mut self, v: f32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_f64(&mut self, v: f64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_char(&mut self, v: char) {
        let mut buf = [0u8; 4];
        let s = v.encode_utf8(&mut buf);
        self.write_bytes(s.as_bytes());
    }

    /// Writes a length-prefixed byte slice: [len: u32][bytes...]  
    fn write_bytes(&mut self, v: &[u8]) {
        let len = v.len() as u32;
        self.buf.extend_from_slice(&len.to_le_bytes());
        self.buf.extend_from_slice(v);
    }

    /// Writes a length-prefixed string: [len: u32][utf8_bytes...]  
    fn write_string(&mut self, v: &str) {
        self.write_bytes(v.as_bytes());
    }

    fn into_bytes(self) -> Vec<u8> {
        self.buf
    }
}

// ============================================================================
// SchemaSerializer — the top-level serde::Serializer
// ============================================================================

pub struct SchemaSerializer {
    schema: SchemaWriter,
    values: ValueWriter,
}

impl SchemaSerializer {
    pub fn new() -> Self {
        Self {
            schema: SchemaWriter::new(),
            values: ValueWriter::new(),
        }
    }
}

/// The Ok output of the serializer: schema bytes + value bytes (before framing)  
type SerOutput = (Vec<u8>, Vec<u8>);

impl<'a> ser::Serializer for &'a mut SchemaSerializer {
    type Ok = ();
    type Error = SchemaSerdeError;

    // Associated types for compound data structures
    type SerializeSeq = SeqSerializer<'a>;
    type SerializeTuple = TupleSerializer<'a>;
    type SerializeTupleStruct = TupleSerializer<'a>;
    type SerializeTupleVariant = TupleVariantSerializer<'a>;
    type SerializeMap = MapSerializer<'a>;
    type SerializeStruct = StructSerializer<'a>;
    type SerializeStructVariant = StructVariantSerializer<'a>;

    // --- Primitives ---

    fn serialize_bool(self, v: bool) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::Bool);
        self.values.write_bool(v);
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::I8);
        self.values.write_i8(v);
        Ok(())
    }

    fn serialize_i16(self, v: i16) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::I16);
        self.values.write_i16(v);
        Ok(())
    }

    fn serialize_i32(self, v: i32) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::I32);
        self.values.write_i32(v);
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::I64);
        self.values.write_i64(v);
        Ok(())
    }

    fn serialize_i128(self, v: i128) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::I128);
        self.values.write_i128(v);
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::U8);
        self.values.write_u8(v);
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::U16);
        self.values.write_u16(v);
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::U32);
        self.values.write_u32(v);
        Ok(())
    }

    fn serialize_u64(self, v: u64) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::U64);
        self.values.write_u64(v);
        Ok(())
    }

    fn serialize_u128(self, v: u128) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::U128);
        self.values.write_u128(v);
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::F32);
        self.values.write_f32(v);
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::F64);
        self.values.write_f64(v);
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::Char);
        self.values.write_char(v);
        Ok(())
    }

    // --- String & Bytes ---

    fn serialize_str(self, v: &str) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::String);
        self.values.write_string(v);
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::Bytes);
        self.values.write_bytes(v);
        Ok(())
    }

    // --- Option ---

    fn serialize_none(self) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::None);
        // No value bytes for None
        Ok(())
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::Some);
        // The inner value's schema + value follow inline
        value.serialize(self)
    }

    // --- Unit ---

    fn serialize_unit(self) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::Unit);
        Ok(())
    }

    /// Unit structs (e.g., `struct Foo;`) — treated as Unit  
    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), Self::Error> {
        self.serialize_unit()
    }

    /// Unit variant of an enum (e.g., `FileType::Dir`)  
    /// Schema: [Enum tag][variant_index: u32][variant_name][Unit tag]  
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::Enum);
        self.schema.write_u32(variant_index);
        self.schema.write_field_name(variant);
        self.schema.write_tag(TypeTag::Unit);
        // No value bytes — variant index already encodes the value
        Ok(())
    }

    /// Newtype struct (e.g., transparent wrapper) — serialize inner directly  
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        value.serialize(self)
    }

    /// Newtype variant of an enum: `JournalEntry::Mkdir(MkdirEntry)`  
    /// Schema: [Enum tag][variant_index: u32][variant_name][inner schema...]  
    /// Value:  [inner value bytes...]  
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.schema.write_tag(TypeTag::Enum);
        self.schema.write_u32(variant_index);
        self.schema.write_field_name(variant);
        // Inner value schema + bytes follow
        value.serialize(self)
    }

    // --- Sequences (Vec<T>) ---

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let count = len.unwrap_or(0) as u32;
        self.schema.write_tag(TypeTag::Seq);
        self.schema.write_u32(count);
        // Each element will append its own schema + values
        Ok(SeqSerializer {
            ser: self,
            count: 0,
        })
    }

    // --- Tuples ---

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.schema.write_tag(TypeTag::Tuple);
        self.schema.write_u16(len as u16);
        Ok(TupleSerializer { ser: self })
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_tuple(len)
    }

    /// Tuple variant: `InodeView::File(String, InodeFile)`  
    /// Schema: [Enum][variant_index][variant_name][Tuple][elem_count][elem schemas...]  
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.schema.write_tag(TypeTag::Enum);
        self.schema.write_u32(variant_index);
        self.schema.write_field_name(variant);
        self.schema.write_tag(TypeTag::Tuple);
        self.schema.write_u16(len as u16);
        Ok(TupleVariantSerializer { ser: self })
    }

    // --- Maps (HashMap<K, V>) ---

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let count = len.unwrap_or(0) as u32;
        self.schema.write_tag(TypeTag::Map);
        self.schema.write_u32(count);
        Ok(MapSerializer { ser: self })
    }

    // --- Structs ---

    /// Named struct: `JournalBatch { seq_id, batch }`  
    /// Schema: [Struct tag][field_count: u16] then per-field: [field_name][field schema...]  
    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.schema.write_tag(TypeTag::Struct);
        self.schema.write_u16(len as u16);
        Ok(StructSerializer { ser: self })
    }

    /// Struct variant of an enum (not currently used in curvine, but needed for completeness)  
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.schema.write_tag(TypeTag::Enum);
        self.schema.write_u32(variant_index);
        self.schema.write_field_name(variant);
        self.schema.write_tag(TypeTag::Struct);
        self.schema.write_u16(len as u16);
        Ok(StructVariantSerializer { ser: self })
    }
}

// ============================================================================
// SerializeSeq — handles Vec<T>
// ============================================================================

pub struct SeqSerializer<'a> {
    ser: &'a mut SchemaSerializer,
    count: u32,
}

impl<'a> ser::SerializeSeq for SeqSerializer<'a> {
    type Ok = ();
    type Error = SchemaSerdeError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        // Each element writes its own schema entry + value bytes
        // This means heterogeneous schemas per element (needed for enum vecs like Vec<JournalEntry>)
        value.serialize(&mut *self.ser)?;
        self.count += 1;
        Ok(())
    }

    fn end(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

// ============================================================================
// SerializeTuple — handles (K, V) tuples
// ============================================================================

pub struct TupleSerializer<'a> {
    ser: &'a mut SchemaSerializer,
}

impl<'a> ser::SerializeTuple for TupleSerializer<'a> {
    type Ok = ();
    type Error = SchemaSerdeError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.ser)
    }

    fn end(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl<'a> ser::SerializeTupleStruct for TupleSerializer<'a> {
    type Ok = ();
    type Error = SchemaSerdeError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.ser)
    }

    fn end(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

// ============================================================================
// TupleVariantSerializer — handles InodeView::File(String, InodeFile)
// ============================================================================

pub struct TupleVariantSerializer<'a> {
    ser: &'a mut SchemaSerializer,
}

impl<'a> ser::SerializeTupleVariant for TupleVariantSerializer<'a> {
    type Ok = ();
    type Error = SchemaSerdeError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.ser)
    }

    fn end(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

// ============================================================================
// MapSerializer — handles HashMap<K, V>
// ============================================================================

pub struct MapSerializer<'a> {
    ser: &'a mut SchemaSerializer,
}

impl<'a> ser::SerializeMap for MapSerializer<'a> {
    type Ok = ();
    type Error = SchemaSerdeError;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), Self::Error> {
        key.serialize(&mut *self.ser)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.ser)
    }

    fn end(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

// ============================================================================
// StructSerializer — handles named structs like JournalBatch, InodeFile, etc.
// ============================================================================

pub struct StructSerializer<'a> {
    ser: &'a mut SchemaSerializer,
}

impl<'a> ser::SerializeStruct for StructSerializer<'a> {
    type Ok = ();
    type Error = SchemaSerdeError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        // Schema: write field name, then the field's type schema follows from value.serialize()
        self.ser.schema.write_field_name(key);
        value.serialize(&mut *self.ser)
    }

    fn end(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

// ============================================================================
// StructVariantSerializer — handles struct variants in enums
// ============================================================================

pub struct StructVariantSerializer<'a> {
    ser: &'a mut SchemaSerializer,
}

impl<'a> ser::SerializeStructVariant for StructVariantSerializer<'a> {
    type Ok = ();
    type Error = SchemaSerdeError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.ser.schema.write_field_name(key);
        value.serialize(&mut *self.ser)
    }

    fn end(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

// ============================================================================
// Top-level serialize function
// ============================================================================

/// Serializes a value into the custom `[schema_len][schema][value_len][value]` format.  
pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, SchemaSerdeError> {
    let mut serializer = SchemaSerializer::new();
    value.serialize(&mut serializer)?;

    let schema_bytes = serializer.schema.into_bytes();
    let value_bytes = serializer.values.into_bytes();

    // Wire format: [schema_len: u32][schema...][value_len: u32][values...]
    let total = 4 + schema_bytes.len() + 4 + value_bytes.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&(schema_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(&schema_bytes);
    buf.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(&value_bytes);
    Ok(buf)
}

pub struct SchemaDeserializer<'de> {
    schema: &'de [u8],
    values: &'de [u8],
    schema_pos: usize,
    values_pos: usize,
}

impl<'de> SchemaDeserializer<'de> {
    fn new(schema: &'de [u8], values: &'de [u8]) -> Self {
        Self {
            schema,
            values,
            schema_pos: 0,
            values_pos: 0,
        }
    }

    // --- Schema readers ---

    fn read_tag(&mut self) -> Result<TypeTag, SchemaSerdeError> {
        if self.schema_pos >= self.schema.len() {
            return Err(SchemaSerdeError("unexpected end of schema".into()));
        }
        let byte = self.schema[self.schema_pos];
        self.schema_pos += 1;
        TypeTag::from_u8(byte)
    }

    fn read_schema_u16(&mut self) -> Result<u16, SchemaSerdeError> {
        if self.schema_pos + 2 > self.schema.len() {
            return Err(SchemaSerdeError(
                "unexpected end of schema reading u16".into(),
            ));
        }
        let v = u16::from_le_bytes(
            self.schema[self.schema_pos..self.schema_pos + 2]
                .try_into()
                .unwrap(),
        );
        self.schema_pos += 2;
        Ok(v)
    }

    fn read_schema_u32(&mut self) -> Result<u32, SchemaSerdeError> {
        if self.schema_pos + 4 > self.schema.len() {
            return Err(SchemaSerdeError(
                "unexpected end of schema reading u32".into(),
            ));
        }
        let v = u32::from_le_bytes(
            self.schema[self.schema_pos..self.schema_pos + 4]
                .try_into()
                .unwrap(),
        );
        self.schema_pos += 4;
        Ok(v)
    }

    fn read_field_name(&mut self) -> Result<&'de str, SchemaSerdeError> {
        let len = self.read_schema_u16()? as usize;
        if self.schema_pos + len > self.schema.len() {
            return Err(SchemaSerdeError(
                "unexpected end of schema reading field name".into(),
            ));
        }
        let name = std::str::from_utf8(&self.schema[self.schema_pos..self.schema_pos + len])
            .map_err(|e| SchemaSerdeError(format!("invalid utf8 in field name: {}", e)))?;
        self.schema_pos += len;
        Ok(name)
    }

    // --- Value readers ---

    fn read_value_bytes(&mut self, n: usize) -> Result<&'de [u8], SchemaSerdeError> {
        if self.values_pos + n > self.values.len() {
            return Err(SchemaSerdeError(format!(
                "unexpected end of values: need {} bytes at pos {}, have {}",
                n,
                self.values_pos,
                self.values.len()
            )));
        }
        let slice = &self.values[self.values_pos..self.values_pos + n];
        self.values_pos += n;
        Ok(slice)
    }

    fn read_value_bool(&mut self) -> Result<bool, SchemaSerdeError> {
        let b = self.read_value_bytes(1)?;
        Ok(b[0] != 0)
    }

    fn read_value_i8(&mut self) -> Result<i8, SchemaSerdeError> {
        let b = self.read_value_bytes(1)?;
        Ok(i8::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_value_i16(&mut self) -> Result<i16, SchemaSerdeError> {
        let b = self.read_value_bytes(2)?;
        Ok(i16::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_value_i32(&mut self) -> Result<i32, SchemaSerdeError> {
        let b = self.read_value_bytes(4)?;
        Ok(i32::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_value_i64(&mut self) -> Result<i64, SchemaSerdeError> {
        let b = self.read_value_bytes(8)?;
        Ok(i64::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_value_i128(&mut self) -> Result<i128, SchemaSerdeError> {
        let b = self.read_value_bytes(16)?;
        Ok(i128::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_value_u8(&mut self) -> Result<u8, SchemaSerdeError> {
        let b = self.read_value_bytes(1)?;
        Ok(b[0])
    }

    fn read_value_u16(&mut self) -> Result<u16, SchemaSerdeError> {
        let b = self.read_value_bytes(2)?;
        Ok(u16::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_value_u32(&mut self) -> Result<u32, SchemaSerdeError> {
        let b = self.read_value_bytes(4)?;
        Ok(u32::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_value_u64(&mut self) -> Result<u64, SchemaSerdeError> {
        let b = self.read_value_bytes(8)?;
        Ok(u64::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_value_u128(&mut self) -> Result<u128, SchemaSerdeError> {
        let b = self.read_value_bytes(16)?;
        Ok(u128::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_value_f32(&mut self) -> Result<f32, SchemaSerdeError> {
        let b = self.read_value_bytes(4)?;
        Ok(f32::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_value_f64(&mut self) -> Result<f64, SchemaSerdeError> {
        let b = self.read_value_bytes(8)?;
        Ok(f64::from_le_bytes(b.try_into().unwrap()))
    }

    /// Reads a length-prefixed string from values: [len: u32][utf8...]  
    fn read_value_string(&mut self) -> Result<&'de str, SchemaSerdeError> {
        let len = {
            let b = self.read_value_bytes(4)?;
            u32::from_le_bytes(b.try_into().unwrap()) as usize
        };
        let b = self.read_value_bytes(len)?;
        std::str::from_utf8(b)
            .map_err(|e| SchemaSerdeError(format!("invalid utf8 in value string: {}", e)))
    }

    /// Reads length-prefixed bytes from values: [len: u32][bytes...]  
    fn read_value_byte_buf(&mut self) -> Result<&'de [u8], SchemaSerdeError> {
        let len = {
            let b = self.read_value_bytes(4)?;
            u32::from_le_bytes(b.try_into().unwrap()) as usize
        };
        self.read_value_bytes(len)
    }

    /// Reads a char (stored as length-prefixed utf8 bytes)  
    fn read_value_char(&mut self) -> Result<char, SchemaSerdeError> {
        let s = self.read_value_string()?;
        s.chars()
            .next()
            .ok_or_else(|| SchemaSerdeError("empty char value".into()))
    }
}

// ============================================================================
// serde::Deserializer implementation
// ============================================================================

impl<'de, 'a> serde::Deserializer<'de> for &'a mut SchemaDeserializer<'de> {
    type Error = SchemaSerdeError;

    /// Schema-driven deserialization: reads the type tag, then dispatches.  
    fn deserialize_any<V: serde::de::Visitor<'de>>(
        self,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        let tag = self.read_tag()?;
        match tag {
            TypeTag::Bool => visitor.visit_bool(self.read_value_bool()?),
            TypeTag::I8 => visitor.visit_i8(self.read_value_i8()?),
            TypeTag::I16 => visitor.visit_i16(self.read_value_i16()?),
            TypeTag::I32 => visitor.visit_i32(self.read_value_i32()?),
            TypeTag::I64 => visitor.visit_i64(self.read_value_i64()?),
            TypeTag::I128 => visitor.visit_i128(self.read_value_i128()?),
            TypeTag::U8 => visitor.visit_u8(self.read_value_u8()?),
            TypeTag::U16 => visitor.visit_u16(self.read_value_u16()?),
            TypeTag::U32 => visitor.visit_u32(self.read_value_u32()?),
            TypeTag::U64 => visitor.visit_u64(self.read_value_u64()?),
            TypeTag::U128 => visitor.visit_u128(self.read_value_u128()?),
            TypeTag::F32 => visitor.visit_f32(self.read_value_f32()?),
            TypeTag::F64 => visitor.visit_f64(self.read_value_f64()?),
            TypeTag::Char => visitor.visit_char(self.read_value_char()?),
            TypeTag::String => visitor.visit_borrowed_str(self.read_value_string()?),
            TypeTag::Bytes => visitor.visit_borrowed_bytes(self.read_value_byte_buf()?),
            TypeTag::Unit => visitor.visit_unit(),
            TypeTag::None => visitor.visit_none(),
            TypeTag::Some => visitor.visit_some(self),
            TypeTag::Seq => {
                let count = self.read_schema_u32()? as usize;
                visitor.visit_seq(SeqAccess {
                    de: self,
                    remaining: count,
                })
            }
            TypeTag::Tuple => {
                let count = self.read_schema_u16()? as usize;
                visitor.visit_seq(SeqAccess {
                    de: self,
                    remaining: count,
                })
            }
            TypeTag::Map => {
                let count = self.read_schema_u32()? as usize;
                visitor.visit_map(MapAccess {
                    de: self,
                    remaining: count,
                })
            }
            TypeTag::Struct => {
                let field_count = self.read_schema_u16()? as usize;
                visitor.visit_map(StructAccess {
                    de: self,
                    remaining: field_count,
                    current_key: None,
                })
            }
            TypeTag::Enum => {
                let variant_index = self.read_schema_u32()?;
                let variant_name = self.read_field_name()?;
                visitor.visit_enum(EnumAccess {
                    de: self,
                    variant_index,
                    variant_name,
                })
            }
        }
    }

    // All type-specific deserialize_* methods forward to deserialize_any,
    // which is schema-driven rather than type-hint-driven.
    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

// Also implement de::Error for SchemaSerdeError
impl serde::de::Error for SchemaSerdeError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        SchemaSerdeError(msg.to_string())
    }
}

// ============================================================================
// SeqAccess — deserializes Vec<T> and tuples
// ============================================================================

struct SeqAccess<'a, 'de> {
    de: &'a mut SchemaDeserializer<'de>,
    remaining: usize,
}

impl<'a, 'de> serde::de::SeqAccess<'de> for SeqAccess<'a, 'de> {
    type Error = SchemaSerdeError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(&mut *self.de).map(Some)
    }
}

// ============================================================================
// MapAccess — deserializes HashMap<K, V>
// ============================================================================

struct MapAccess<'a, 'de> {
    de: &'a mut SchemaDeserializer<'de>,
    remaining: usize,
}

impl<'a, 'de> serde::de::MapAccess<'de> for MapAccess<'a, 'de> {
    type Error = SchemaSerdeError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(&mut *self.de).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self.de)
    }
}

// ============================================================================
// StructAccess — deserializes named structs via MapAccess
// Reads field names from schema, values from value buffer
// ============================================================================

struct StructAccess<'a, 'de> {
    de: &'a mut SchemaDeserializer<'de>,
    remaining: usize,
    current_key: Option<&'de str>,
}

impl<'a, 'de> serde::de::MapAccess<'de> for StructAccess<'a, 'de> {
    type Error = SchemaSerdeError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        // Read field name from schema
        let field_name = self.de.read_field_name()?;
        self.current_key = Some(field_name);
        // Deserialize the field name as a string key
        seed.deserialize(FieldNameDeserializer(field_name))
            .map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        // The next call to deserialize_any on self.de reads the field's type tag + value
        seed.deserialize(&mut *self.de)
    }
}

/// Tiny deserializer that yields a borrowed str — used for struct field name keys.  
struct FieldNameDeserializer<'de>(&'de str);

impl<'de> serde::Deserializer<'de> for FieldNameDeserializer<'de> {
    type Error = SchemaSerdeError;

    fn deserialize_any<V: serde::de::Visitor<'de>>(
        self,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_borrowed_str(self.0)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

// ============================================================================
// EnumAccess — deserializes enums (unit, newtype, tuple, struct variants)
// ============================================================================

struct EnumAccess<'a, 'de> {
    de: &'a mut SchemaDeserializer<'de>,
    variant_index: u32,
    variant_name: &'de str,
}

impl<'a, 'de> serde::de::EnumAccess<'de> for EnumAccess<'a, 'de> {
    type Error = SchemaSerdeError;
    type Variant = VariantAccess<'a, 'de>;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        // Deserialize variant identifier (the variant name)
        let val = seed.deserialize(FieldNameDeserializer(self.variant_name))?;
        Ok((val, VariantAccess { de: self.de }))
    }
}

struct VariantAccess<'a, 'de> {
    de: &'a mut SchemaDeserializer<'de>,
}

impl<'a, 'de> serde::de::VariantAccess<'de> for VariantAccess<'a, 'de> {
    type Error = SchemaSerdeError;

    /// Unit variant — schema already has [Unit] tag, just consume it  
    fn unit_variant(self) -> Result<(), Self::Error> {
        // The next tag in schema should be Unit (already peeked by deserialize_any)
        // But we're called after EnumAccess::variant_seed, so the tag is next
        let tag = self.de.read_tag()?;
        if tag != TypeTag::Unit {
            return Err(SchemaSerdeError(format!(
                "expected Unit tag for unit variant, got {:?}",
                tag
            )));
        }
        Ok(())
    }

    /// Newtype variant — `JournalEntry::Mkdir(MkdirEntry)`  
    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self.de)
    }

    /// Tuple variant — `InodeView::File(String, InodeFile)`  
    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // Next in schema is [Tuple][elem_count], then element schemas
        serde::de::Deserializer::deserialize_any(&mut *self.de, visitor)
    }

    /// Struct variant — not currently used in curvine but complete for correctness  
    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        serde::de::Deserializer::deserialize_any(&mut *self.de, visitor)
    }
}

// ============================================================================
// Top-level deserialize function
// ============================================================================

/// Deserializes from the custom `[schema_len][schema][value_len][values]` wire format.  
pub fn deserialize<'a, T>(bytes: &'a [u8]) -> Result<T, SchemaSerdeError>
where
    T: serde::de::Deserialize<'a>,
{
    if bytes.len() < 4 {
        return Err(SchemaSerdeError("input too short for schema_len".into()));
    }

    // Read schema
    let schema_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if bytes.len() < 4 + schema_len {
        return Err(SchemaSerdeError("input too short for schema bytes".into()));
    }
    let schema = &bytes[4..4 + schema_len];

    // Read values
    let val_offset = 4 + schema_len;
    if bytes.len() < val_offset + 4 {
        return Err(SchemaSerdeError("input too short for value_len".into()));
    }
    let value_len =
        u32::from_le_bytes(bytes[val_offset..val_offset + 4].try_into().unwrap()) as usize;
    if bytes.len() < val_offset + 4 + value_len {
        return Err(SchemaSerdeError("input too short for value bytes".into()));
    }
    let values = &bytes[val_offset + 4..val_offset + 4 + value_len];

    let mut de = SchemaDeserializer::new(schema, values);
    let result = T::deserialize(&mut de)?;
    Ok(result)
}

// curvine-common/src/utils/schema_serde.rs — append at bottom

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    // ========================================================================
    // Helper: round-trip assertion
    // ========================================================================
    fn round_trip<T>(value: &T) -> T
    where
        T: Serialize + for<'de> Deserialize<'de> + std::fmt::Debug,
    {
        let bytes = serialize(value).expect("serialize failed");
        // Verify wire format: [schema_len(4)] [schema...] [value_len(4)] [values...]
        assert!(bytes.len() >= 8, "wire bytes too short");
        let schema_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let values_offset = 4 + schema_len;
        assert!(bytes.len() >= values_offset + 4, "value header missing");
        deserialize::<T>(&bytes).expect("deserialize failed")
    }

    fn assert_round_trip<T>(value: &T)
    where
        T: Serialize + for<'de> Deserialize<'de> + std::fmt::Debug + PartialEq,
    {
        let result = round_trip(value);
        assert_eq!(value, &result, "round-trip mismatch");
    }

    // ========================================================================
    // 1. Primitives
    // ========================================================================
    #[test]
    fn test_bool() {
        assert_round_trip(&true);
        assert_round_trip(&false);
    }

    #[test]
    fn test_integers() {
        assert_round_trip(&42i8);
        assert_round_trip(&(-100i16));
        assert_round_trip(&1234i32);
        assert_round_trip(&u64::MAX);
        assert_round_trip(&i64::MIN);
        assert_round_trip(&0u8);
        assert_round_trip(&255u8);
        assert_round_trip(&0u16);
        assert_round_trip(&65535u16);
        assert_round_trip(&0u32);
        assert_round_trip(&u32::MAX);
    }

    #[test]
    fn test_floats() {
        assert_round_trip(&3.14f32);
        assert_round_trip(&std::f64::consts::PI);
        assert_round_trip(&0.0f32);
        assert_round_trip(&f64::NEG_INFINITY);
    }

    #[test]
    fn test_char() {
        assert_round_trip(&'a');
        assert_round_trip(&'Z');
    }

    // ========================================================================
    // 2. String & Bytes
    // ========================================================================
    #[test]
    fn test_string() {
        assert_round_trip(&String::from("hello world"));
        assert_round_trip(&String::from(""));
        assert_round_trip(&String::from("/a/b/c"));
        assert_round_trip(&String::from("s3://bucket/path/to/file.txt"));
    }

    // ========================================================================
    // 3. Option
    // ========================================================================
    #[test]
    fn test_option_some() {
        let v: Option<String> = Some("test".into());
        assert_round_trip(&v);
    }

    #[test]
    fn test_option_none() {
        let v: Option<String> = None;
        assert_round_trip(&v);
    }

    #[test]
    fn test_option_some_i64() {
        let v: Option<i64> = Some(12345);
        assert_round_trip(&v);
    }

    #[test]
    fn test_option_none_i64() {
        let v: Option<i64> = None;
        assert_round_trip(&v);
    }

    // ========================================================================
    // 4. Vec / Seq
    // ========================================================================
    #[test]
    fn test_vec_empty() {
        let v: Vec<i32> = vec![];
        assert_round_trip(&v);
    }

    #[test]
    fn test_vec_primitives() {
        assert_round_trip(&vec![1i32, 2, 3, 4, 5]);
        assert_round_trip(&vec![10u64, 20, 30]);
    }

    #[test]
    fn test_vec_strings() {
        assert_round_trip(&vec![
            "hello".to_string(),
            "world".to_string(),
            "".to_string(),
        ]);
    }

    // ========================================================================
    // 5. Tuple — mirrors (K, V) used in hash_app_storage.rs
    // ========================================================================
    #[test]
    fn test_tuple_pair() {
        let pair: (i64, String) = (42, "value".into());
        assert_round_trip(&pair);
    }

    #[test]
    fn test_tuple_triple() {
        let triple: (u32, String, bool) = (1, "test".into(), true);
        assert_round_trip(&triple);
    }

    // ========================================================================
    // 6. HashMap / Map — mirrors x_attr: HashMap<String, Vec<u8>>
    // ========================================================================
    #[test]
    fn test_hashmap_empty() {
        let map: HashMap<String, i32> = HashMap::new();
        assert_round_trip(&map);
    }

    #[test]
    fn test_hashmap_string_to_i32() {
        let mut map = HashMap::new();
        map.insert("key1".to_string(), 42i32);
        map.insert("key2".to_string(), 84i32);
        assert_round_trip(&map);
    }

    #[test]
    fn test_hashmap_string_to_vec_u8() {
        // Mirrors x_attr: HashMap<String, Vec<u8>>
        let mut map: HashMap<String, Vec<u8>> = HashMap::new();
        map.insert("attr1".to_string(), vec![1, 2, 3]);
        map.insert("attr2".to_string(), vec![]);
        assert_round_trip(&map);
    }

    // ========================================================================
    // 7. Simple struct — mirrors BlockLocation
    // ========================================================================
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestBlockLocation {
        worker_id: u32,
        storage_type: TestStorageType,
    }

    #[test]
    fn test_simple_struct() {
        let loc = TestBlockLocation {
            worker_id: 5,
            storage_type: TestStorageType::Ssd,
        };
        assert_round_trip(&loc);
    }

    // ========================================================================
    // 8. Unit enum — mirrors StorageType, FileType, TtlAction
    // ========================================================================
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[repr(i32)]
    enum TestStorageType {
        Mem = 0,
        Ssd = 1,
        Hdd = 2,
        Ufs = 3,
        Disk = 4,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[repr(i8)]
    enum TestFileType {
        Dir = 0,
        File = 1,
        Link = 2,
        Stream = 3,
    }

    #[test]
    fn test_unit_enum_variants() {
        assert_round_trip(&TestStorageType::Mem);
        assert_round_trip(&TestStorageType::Ssd);
        assert_round_trip(&TestStorageType::Disk);
        assert_round_trip(&TestFileType::Dir);
        assert_round_trip(&TestFileType::File);
        assert_round_trip(&TestFileType::Link);
    }

    // ========================================================================
    // 9. StoragePolicy equivalent — nested struct with enums
    // ========================================================================
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestStoragePolicy {
        storage_type: TestStorageType,
        ttl_ms: i64,
        ttl_action: TestTtlAction,
        ufs_mtime: i64,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[repr(i8)]
    enum TestTtlAction {
        None = 0,
        Delete = 1,
        Free = 2,
    }

    #[test]
    fn test_storage_policy() {
        let policy = TestStoragePolicy {
            storage_type: TestStorageType::Ssd,
            ttl_ms: 604800000,
            ttl_action: TestTtlAction::Delete,
            ufs_mtime: 1234567890,
        };
        assert_round_trip(&policy);
    }

    // ========================================================================
    // 10. Struct with Option fields — mirrors LoadJobCommand
    // ========================================================================
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
    struct TestLoadJobCommand {
        source_path: String,
        target_path: Option<String>,
        replicas: Option<i32>,
        block_size: Option<i64>,
        storage_type: Option<TestStorageType>,
        ttl_ms: Option<i64>,
        overwrite: Option<bool>,
    }

    #[test]
    fn test_struct_with_options_all_some() {
        let cmd = TestLoadJobCommand {
            source_path: "s3://bucket/file.csv".into(),
            target_path: Some("/curvine/data".into()),
            replicas: Some(3),
            block_size: Some(67108864),
            storage_type: Some(TestStorageType::Ssd),
            ttl_ms: Some(604800000),
            overwrite: Some(true),
        };
        assert_round_trip(&cmd);
    }

    #[test]
    fn test_struct_with_options_all_none() {
        let cmd = TestLoadJobCommand {
            source_path: "/data/input".into(),
            ..Default::default()
        };
        assert_round_trip(&cmd);
    }

    #[test]
    fn test_struct_with_options_mixed() {
        let cmd = TestLoadJobCommand {
            source_path: "/src".into(),
            target_path: None,
            replicas: Some(2),
            block_size: None,
            storage_type: Some(TestStorageType::Mem),
            ttl_ms: None,
            overwrite: None,
        };
        assert_round_trip(&cmd);
    }

    // ========================================================================
    // 11. Struct with Vec + nested struct — mirrors BlockMeta (master)
    // ========================================================================
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestBlockMeta {
        id: i64,
        len: u32,
        replicas: u8,
        locs: Option<Vec<TestBlockLocation>>,
    }

    #[test]
    fn test_struct_with_vec_of_structs() {
        let meta = TestBlockMeta {
            id: 100,
            len: 67108864,
            replicas: 3,
            locs: Some(vec![
                TestBlockLocation {
                    worker_id: 1,
                    storage_type: TestStorageType::Ssd,
                },
                TestBlockLocation {
                    worker_id: 2,
                    storage_type: TestStorageType::Hdd,
                },
            ]),
        };
        assert_round_trip(&meta);
    }

    #[test]
    fn test_struct_with_none_vec() {
        let meta = TestBlockMeta {
            id: 200,
            len: 0,
            replicas: 1,
            locs: None,
        };
        assert_round_trip(&meta);
    }

    // ========================================================================
    // 12. AclFeature / FileFeature equivalent — nested structs with HashMap
    // ========================================================================
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestAclFeature {
        owner: String,
        group: String,
        mode: u32,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestFileFeature {
        x_attr: HashMap<String, Vec<u8>>,
        file_write: Option<String>, // simplified WriteFeature
        acl: TestAclFeature,
    }

    #[test]
    fn test_file_feature_with_attrs() {
        let mut x_attr = HashMap::new();
        x_attr.insert("checksum".into(), vec![0xDE, 0xAD, 0xBE, 0xEF]);
        x_attr.insert("encoding".into(), b"utf-8".to_vec());

        let feature = TestFileFeature {
            x_attr,
            file_write: Some("client_1".into()),
            acl: TestAclFeature {
                owner: "root".into(),
                group: "wheel".into(),
                mode: 0o755,
            },
        };
        assert_round_trip(&feature);
    }

    #[test]
    fn test_file_feature_empty() {
        let feature = TestFileFeature {
            x_attr: HashMap::new(),
            file_write: None,
            acl: TestAclFeature {
                owner: "".into(),
                group: "".into(),
                mode: 0o777,
            },
        };
        assert_round_trip(&feature);
    }

    // ========================================================================
    // 13. Newtype enum — mirrors JournalEntry
    // ========================================================================
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestDeleteEntry {
        op_ms: u64,
        path: String,
        mtime: i64,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestRenameEntry {
        op_ms: u64,
        src: String,
        dst: String,
        mtime: i64,
        flags: u32,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestMkdirEntry {
        op_ms: u64,
        path: String,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum TestJournalEntry {
        Mkdir(TestMkdirEntry),
        Delete(TestDeleteEntry),
        Rename(TestRenameEntry),
    }

    #[test]
    fn test_newtype_enum_variant_mkdir() {
        let entry = TestJournalEntry::Mkdir(TestMkdirEntry {
            op_ms: 1000,
            path: "/a/b/c".into(),
        });
        assert_round_trip(&entry);
    }

    #[test]
    fn test_newtype_enum_variant_delete() {
        let entry = TestJournalEntry::Delete(TestDeleteEntry {
            op_ms: 2000,
            path: "/x/y/z".into(),
            mtime: 9999,
        });
        assert_round_trip(&entry);
    }

    #[test]
    fn test_newtype_enum_variant_rename() {
        let entry = TestJournalEntry::Rename(TestRenameEntry {
            op_ms: 3000,
            src: "/old/path".into(),
            dst: "/new/path".into(),
            mtime: 5000,
            flags: 1,
        });
        assert_round_trip(&entry);
    }

    // ========================================================================
    // 14. JournalBatch equivalent — struct with Vec<enum>
    // ========================================================================
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestJournalBatch {
        seq_id: u64,
        batch: Vec<TestJournalEntry>,
    }

    #[test]
    fn test_journal_batch_empty() {
        let batch = TestJournalBatch {
            seq_id: 1,
            batch: vec![],
        };
        assert_round_trip(&batch);
    }

    #[test]
    fn test_journal_batch_mixed_entries() {
        let batch = TestJournalBatch {
            seq_id: 42,
            batch: vec![
                TestJournalEntry::Mkdir(TestMkdirEntry {
                    op_ms: 100,
                    path: "/dir1".into(),
                }),
                TestJournalEntry::Delete(TestDeleteEntry {
                    op_ms: 200,
                    path: "/old_file".into(),
                    mtime: 500,
                }),
                TestJournalEntry::Rename(TestRenameEntry {
                    op_ms: 300,
                    src: "/a".into(),
                    dst: "/b".into(),
                    mtime: 600,
                    flags: 0,
                }),
            ],
        };
        assert_round_trip(&batch);
    }

    // ========================================================================
    // 15. Tuple enum — mirrors InodeView::File(String, InodeFile)
    // ========================================================================
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestInodeFile {
        id: i64,
        parent_id: i64,
        file_type: TestFileType,
        mtime: i64,
        len: i64,
        block_size: u32,
        replicas: u8,
        blocks: Vec<TestBlockMeta>,
        nlink: u32,
        target: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestInodeDir {
        id: i64,
        parent_id: i64,
        mtime: i64,
        nlink: u32,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum TestInodeView {
        File(String, TestInodeFile),
        Dir(String, TestInodeDir),
        FileEntry(String, i64),
    }

    #[test]
    fn test_tuple_variant_file() {
        let view = TestInodeView::File(
            "test.txt".into(),
            TestInodeFile {
                id: 1,
                parent_id: 0,
                file_type: TestFileType::File,
                mtime: 1234567890,
                len: 1024,
                block_size: 67108864,
                replicas: 3,
                blocks: vec![TestBlockMeta {
                    id: 10,
                    len: 1024,
                    replicas: 3,
                    locs: Some(vec![TestBlockLocation {
                        worker_id: 1,
                        storage_type: TestStorageType::Ssd,
                    }]),
                }],
                nlink: 1,
                target: None,
            },
        );
        assert_round_trip(&view);
    }

    #[test]
    fn test_tuple_variant_dir() {
        let view = TestInodeView::Dir(
            "my_dir".into(),
            TestInodeDir {
                id: 2,
                parent_id: 0,
                mtime: 9999,
                nlink: 2,
            },
        );
        assert_round_trip(&view);
    }

    #[test]
    fn test_tuple_variant_file_entry() {
        let view = TestInodeView::FileEntry("link.txt".into(), 42);
        assert_round_trip(&view);
    }

    // ========================================================================
    // 16. InetAddr equivalent — simple struct used in raft
    // ========================================================================
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestInetAddr {
        hostname: String,
        port: u16,
    }

    #[test]
    fn test_inet_addr() {
        let addr = TestInetAddr {
            hostname: "192.168.1.100".into(),
            port: 9090,
        };
        assert_round_trip(&addr);
    }

    // ========================================================================
    // 17. Deeply nested struct — InodeFile full equivalent
    // ========================================================================
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestFullInodeFile {
        id: i64,
        parent_id: i64,
        file_type: TestFileType,
        mtime: i64,
        atime: i64,
        len: i64,
        block_size: u32,
        replicas: u8,
        storage_policy: TestStoragePolicy,
        features: TestFileFeature,
        blocks: Vec<TestBlockMeta>,
        nlink: u32,
        next_seq: u32,
        target: Option<String>,
    }

    #[test]
    fn test_deeply_nested_struct() {
        let mut x_attr = HashMap::new();
        x_attr.insert("key".into(), vec![1, 2, 3]);

        let file = TestFullInodeFile {
            id: 1001,
            parent_id: 500,
            file_type: TestFileType::File,
            mtime: 1700000000000,
            atime: 1700000001000,
            len: 2048,
            block_size: 67108864,
            replicas: 3,
            storage_policy: TestStoragePolicy {
                storage_type: TestStorageType::Ssd,
                ttl_ms: 604800000,
                ttl_action: TestTtlAction::Delete,
                ufs_mtime: 1699999999000,
            },
            features: TestFileFeature {
                x_attr,
                file_write: Some("client_abc".into()),
                acl: TestAclFeature {
                    owner: "admin".into(),
                    group: "users".into(),
                    mode: 0o644,
                },
            },
            blocks: vec![
                TestBlockMeta {
                    id: 10,
                    len: 1024,
                    replicas: 3,
                    locs: Some(vec![
                        TestBlockLocation {
                            worker_id: 1,
                            storage_type: TestStorageType::Ssd,
                        },
                        TestBlockLocation {
                            worker_id: 2,
                            storage_type: TestStorageType::Hdd,
                        },
                    ]),
                },
                TestBlockMeta {
                    id: 11,
                    len: 1024,
                    replicas: 3,
                    locs: None,
                },
            ],
            nlink: 2,
            next_seq: 12,
            target: Some("/link/target".into()),
        };
        assert_round_trip(&file);
    }

    // ========================================================================
    // 18. Unit type
    // ========================================================================
    #[test]
    fn test_unit() {
        assert_round_trip(&());
    }

    // ========================================================================
    // 19. SetAttrOpts equivalent — many Option fields + HashMap + Vec
    // ========================================================================
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
    struct TestSetAttrOpts {
        recursive: bool,
        replicas: Option<i32>,
        owner: Option<String>,
        group: Option<String>,
        mode: Option<u32>,
        atime: Option<i64>,
        mtime: Option<i64>,
        ttl_ms: Option<i64>,
        add_x_attr: HashMap<String, Vec<u8>>,
        remove_x_attr: Vec<String>,
        ufs_mtime: Option<i64>,
    }

    #[test]
    fn test_set_attr_opts_full() {
        let mut add_x = HashMap::new();
        add_x.insert("new_attr".into(), vec![10, 20]);

        let opts = TestSetAttrOpts {
            recursive: true,
            replicas: Some(2),
            owner: Some("new_owner".into()),
            group: None,
            mode: Some(0o755),
            atime: Some(1000),
            mtime: None,
            ttl_ms: Some(3600000),
            add_x_attr: add_x,
            remove_x_attr: vec!["old_attr".into()],
            ufs_mtime: None,
        };
        assert_round_trip(&opts);
    }

    #[test]
    fn test_set_attr_opts_default() {
        let opts = TestSetAttrOpts::default();
        assert_round_trip(&opts);
    }

    // ========================================================================
    // 20. Vec<enum> with different variant shapes
    // ========================================================================
    #[test]
    fn test_vec_of_inode_views() {
        let views = vec![
            TestInodeView::Dir(
                "dir1".into(),
                TestInodeDir {
                    id: 1,
                    parent_id: 0,
                    mtime: 100,
                    nlink: 2,
                },
            ),
            TestInodeView::FileEntry("entry1".into(), 42),
            TestInodeView::File(
                "file1.txt".into(),
                TestInodeFile {
                    id: 3,
                    parent_id: 1,
                    file_type: TestFileType::File,
                    mtime: 200,
                    len: 512,
                    block_size: 64,
                    replicas: 1,
                    blocks: vec![],
                    nlink: 1,
                    target: None,
                },
            ),
        ];
        assert_round_trip(&views);
    }

    // ========================================================================
    // 21. HashMap with struct values — mirrors HashMap<K, V> in app storage
    // ========================================================================
    #[test]
    fn test_hashmap_string_to_struct() {
        let mut map: HashMap<String, TestInetAddr> = HashMap::new();
        map.insert(
            "master".into(),
            TestInetAddr {
                hostname: "10.0.0.1".into(),
                port: 8080,
            },
        );
        map.insert(
            "worker".into(),
            TestInetAddr {
                hostname: "10.0.0.2".into(),
                port: 9090,
            },
        );
        assert_round_trip(&map);
    }

    // ========================================================================
    // 22. Wire format validation — verify [schema_len][schema][value_len][values]
    // ========================================================================
    #[test]
    fn test_wire_format_structure() {
        let value: u64 = 42;
        let bytes = serialize(&value).unwrap();

        // Read schema_len
        let schema_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        assert!(schema_len > 0, "schema should not be empty");

        // Read schema
        let schema = &bytes[4..4 + schema_len];
        // First byte of schema should be TypeTag::U64
        assert_eq!(schema[0], TypeTag::U64 as u8);

        // Read value_len
        let val_offset = 4 + schema_len;
        let value_len =
            u32::from_le_bytes(bytes[val_offset..val_offset + 4].try_into().unwrap()) as usize;
        assert_eq!(value_len, 8, "u64 value should be 8 bytes");

        // Read value
        let val_bytes = &bytes[val_offset + 4..val_offset + 4 + value_len];
        let decoded = u64::from_le_bytes(val_bytes.try_into().unwrap());
        assert_eq!(decoded, 42);

        // Total length check
        assert_eq!(bytes.len(), 4 + schema_len + 4 + value_len);
    }

    #[test]
    fn test_wire_format_string() {
        let value = String::from("abc");
        let bytes = serialize(&value).unwrap();

        let schema_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let schema = &bytes[4..4 + schema_len];
        assert_eq!(schema[0], TypeTag::String as u8);
    }

    #[test]
    fn test_wire_format_struct_schema() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct Small {
            x: i32,
            y: i32,
        }
        let v = Small { x: 1, y: 2 };
        let bytes = serialize(&v).unwrap();

        let schema_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let schema = &bytes[4..4 + schema_len];

        // First byte should be Struct tag
        assert_eq!(schema[0], TypeTag::Struct as u8);
        // Next 2 bytes: field count = 2
        let field_count = u16::from_le_bytes(schema[1..3].try_into().unwrap());
        assert_eq!(field_count, 2);

        let result: Small = deserialize(&bytes).unwrap();
        assert_eq!(result, v);
    }

    // ========================================================================
    // 23. Empty collections
    // ========================================================================
    #[test]
    fn test_empty_hashmap() {
        let map: HashMap<String, String> = HashMap::new();
        assert_round_trip(&map);
    }

    #[test]
    fn test_empty_vec_of_structs() {
        let v: Vec<TestBlockMeta> = vec![];
        assert_round_trip(&v);
    }

    // ========================================================================
    // 24. Error cases
    // ========================================================================
    // 24. Error cases
    // ========================================================================
    #[test]
    fn test_deserialize_empty_bytes() {
        let result = deserialize::<u64>(&[]);
        assert!(result.is_err(), "empty bytes should fail");
    }

    #[test]
    fn test_deserialize_truncated_schema_len() {
        // Only 2 bytes when schema_len needs 4
        let result = deserialize::<u64>(&[0x01, 0x00]);
        assert!(result.is_err(), "truncated schema_len should fail");
    }

    #[test]
    fn test_deserialize_truncated_schema() {
        // schema_len says 10 but only 2 bytes of schema follow
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&10u32.to_le_bytes()); // schema_len = 10
        bytes.extend_from_slice(&[0x00, 0x00]); // only 2 bytes
        let result = deserialize::<u64>(&bytes);
        assert!(result.is_err(), "truncated schema should fail");
    }

    #[test]
    fn test_deserialize_truncated_value_len() {
        // Valid schema_len + schema, but value_len header is missing
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes()); // schema_len = 1
        bytes.push(TypeTag::U64 as u8); // schema: U64
                                        // No value_len or value bytes
        let result = deserialize::<u64>(&bytes);
        assert!(result.is_err(), "missing value_len should fail");
    }

    #[test]
    fn test_deserialize_truncated_value() {
        // Valid schema + value_len says 8, but only 3 value bytes
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes()); // schema_len = 1
        bytes.push(TypeTag::U64 as u8); // schema
        bytes.extend_from_slice(&8u32.to_le_bytes()); // value_len = 8
        bytes.extend_from_slice(&[0x01, 0x02, 0x03]); // only 3 bytes
        let result = deserialize::<u64>(&bytes);
        assert!(result.is_err(), "truncated value should fail");
    }

    #[test]
    fn test_deserialize_invalid_type_tag() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes()); // schema_len = 1
        bytes.push(0xFF); // invalid tag
        bytes.extend_from_slice(&0u32.to_le_bytes()); // value_len = 0
        let result = deserialize::<u64>(&bytes);
        assert!(result.is_err(), "invalid type tag should fail");
    }

    #[test]
    fn test_deserialize_type_mismatch() {
        // Serialize a String, try to deserialize as u64
        let s = String::from("hello");
        let bytes = serialize(&s).unwrap();
        let result = deserialize::<u64>(&bytes);
        assert!(result.is_err(), "type mismatch should fail");
    }

    #[test]
    fn test_deserialize_wrong_struct() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct A {
            x: i32,
            y: i32,
        }

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct B {
            a: String,
            b: String,
        }

        let a = A { x: 1, y: 2 };
        let bytes = serialize(&a).unwrap();
        let result = deserialize::<B>(&bytes);
        assert!(
            result.is_err(),
            "deserializing into wrong struct should fail"
        );
    }

    // ========================================================================
    // 25. Boundary / edge values
    // ========================================================================
    #[test]
    fn test_max_min_values() {
        assert_round_trip(&i8::MIN);
        assert_round_trip(&i8::MAX);
        assert_round_trip(&i16::MIN);
        assert_round_trip(&i16::MAX);
        assert_round_trip(&i32::MIN);
        assert_round_trip(&i32::MAX);
        assert_round_trip(&i64::MIN);
        assert_round_trip(&i64::MAX);
        assert_round_trip(&u8::MIN);
        assert_round_trip(&u8::MAX);
        assert_round_trip(&u16::MIN);
        assert_round_trip(&u16::MAX);
        assert_round_trip(&u32::MIN);
        assert_round_trip(&u32::MAX);
        assert_round_trip(&u64::MIN);
        assert_round_trip(&u64::MAX);
    }

    #[test]
    fn test_float_special_values() {
        assert_round_trip(&f32::INFINITY);
        assert_round_trip(&f32::NEG_INFINITY);
        assert_round_trip(&f64::INFINITY);
        assert_round_trip(&f64::NEG_INFINITY);
        // NaN needs special handling — NaN != NaN
        let bytes = serialize(&f64::NAN).unwrap();
        let result: f64 = deserialize(&bytes).unwrap();
        assert!(result.is_nan());
    }

    #[test]
    fn test_zero_values() {
        assert_round_trip(&0i8);
        assert_round_trip(&0i64);
        assert_round_trip(&0u64);
        assert_round_trip(&0.0f32);
        assert_round_trip(&0.0f64);
    }

    // ========================================================================
    // 26. Large collections — stress test
    // ========================================================================
    #[test]
    fn test_large_vec() {
        let v: Vec<i32> = (0..10_000).collect();
        assert_round_trip(&v);
    }

    #[test]
    fn test_large_string() {
        let s: String = "x".repeat(100_000);
        assert_round_trip(&s);
    }

    #[test]
    fn test_large_hashmap() {
        let mut map: HashMap<String, i64> = HashMap::new();
        for i in 0..1000 {
            map.insert(format!("key_{}", i), i);
        }
        assert_round_trip(&map);
    }

    #[test]
    fn test_large_journal_batch() {
        let batch = TestJournalBatch {
            seq_id: 999,
            batch: (0..500)
                .map(|i| {
                    if i % 3 == 0 {
                        TestJournalEntry::Mkdir(TestMkdirEntry {
                            op_ms: i as u64,
                            path: format!("/dir_{}", i),
                        })
                    } else if i % 3 == 1 {
                        TestJournalEntry::Delete(TestDeleteEntry {
                            op_ms: i as u64,
                            path: format!("/file_{}", i),
                            mtime: i as i64 * 1000,
                        })
                    } else {
                        TestJournalEntry::Rename(TestRenameEntry {
                            op_ms: i as u64,
                            src: format!("/old_{}", i),
                            dst: format!("/new_{}", i),
                            mtime: i as i64 * 2000,
                            flags: i as u32,
                        })
                    }
                })
                .collect(),
        };
        assert_round_trip(&batch);
    }

    // ========================================================================
    // 27. Nested Option<Option<T>>
    // ========================================================================
    #[test]
    fn test_nested_option_some_some() {
        let v: Option<Option<i32>> = Some(Some(42));
        assert_round_trip(&v);
    }

    #[test]
    fn test_nested_option_some_none() {
        let v: Option<Option<i32>> = Some(None);
        assert_round_trip(&v);
    }

    #[test]
    fn test_nested_option_none() {
        let v: Option<Option<i32>> = None;
        assert_round_trip(&v);
    }

    // ========================================================================
    // 28. Vec of tuples — mirrors hash_app_storage usage
    // ========================================================================
    #[test]
    fn test_vec_of_tuples() {
        let v: Vec<(String, i64)> =
            vec![("alpha".into(), 1), ("beta".into(), 2), ("gamma".into(), 3)];
        assert_round_trip(&v);
    }

    // ========================================================================
    // 29. Nested Vec<Vec<T>>
    // ========================================================================
    #[test]
    fn test_nested_vec() {
        let v: Vec<Vec<i32>> = vec![vec![1, 2], vec![], vec![3, 4, 5]];
        assert_round_trip(&v);
    }

    // ========================================================================
    // 30. HashMap<String, Vec<enum>>
    // ========================================================================
    #[test]
    fn test_hashmap_with_enum_vec_values() {
        let mut map: HashMap<String, Vec<TestJournalEntry>> = HashMap::new();
        map.insert(
            "batch_a".into(),
            vec![TestJournalEntry::Mkdir(TestMkdirEntry {
                op_ms: 10,
                path: "/a".into(),
            })],
        );
        map.insert("batch_b".into(), vec![]);
        assert_round_trip(&map);
    }

    // ========================================================================
    // 31. Idempotency — serialize twice, get same bytes
    // ========================================================================
    #[test]
    fn test_serialize_deterministic() {
        let v = TestDeleteEntry {
            op_ms: 100,
            path: "/foo".into(),
            mtime: 999,
        };
        let bytes1 = serialize(&v).unwrap();
        let bytes2 = serialize(&v).unwrap();
        assert_eq!(bytes1, bytes2, "serialization should be deterministic");
    }

    #[test]
    fn test_double_round_trip() {
        let original = TestJournalBatch {
            seq_id: 7,
            batch: vec![TestJournalEntry::Delete(TestDeleteEntry {
                op_ms: 50,
                path: "/tmp".into(),
                mtime: 123,
            })],
        };
        let bytes1 = serialize(&original).unwrap();
        let decoded1: TestJournalBatch = deserialize(&bytes1).unwrap();
        let bytes2 = serialize(&decoded1).unwrap();
        let decoded2: TestJournalBatch = deserialize(&bytes2).unwrap();
        assert_eq!(original, decoded2);
        assert_eq!(bytes1, bytes2);
    }
}
