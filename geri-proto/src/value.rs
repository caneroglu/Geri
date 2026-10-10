use alloc::{collections::BTreeMap, string::String, vec::Vec};
use core::fmt;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
    ser::{SerializeMap, SerializeSeq},
};

#[derive(Clone, Debug, Default, PartialEq)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Map(BTreeMap<String, Value>),
}

pub const MAX_DEPTH: usize = 64;

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        u64::try_from(v).map_or(Self::Int(v), Self::Uint)
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Self::Uint(v)
    }
}

#[derive(Serialize)]
#[serde(rename = "Value", rename_all = "snake_case")]
enum TaggedRef<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(#[serde(serialize_with = "finite_float::serialize")] f64),
    Text(&'a str),
    Bytes(#[serde(serialize_with = "bytes::serialize")] &'a [u8]),
    List(&'a [Value]),
    Map(&'a BTreeMap<String, Value>),
}

#[derive(Deserialize)]
#[serde(rename = "Value", rename_all = "snake_case")]
enum Tagged {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(#[serde(deserialize_with = "finite_float::deserialize")] f64),
    Text(String),
    Bytes(#[serde(deserialize_with = "bytes::deserialize")] Vec<u8>),
    List(Vec<Value>),
    Map(BTreeMap<String, Value>),
}

impl Serialize for Value {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            return match self {
                Self::Null => TaggedRef::Null,
                Self::Bool(b) => TaggedRef::Bool(*b),
                Self::Int(n) => u64::try_from(*n).map_or(TaggedRef::Int(*n), TaggedRef::Uint),
                Self::Uint(n) => TaggedRef::Uint(*n),
                Self::Float(f) => TaggedRef::Float(*f),
                Self::Text(t) => TaggedRef::Text(t),
                Self::Bytes(b) => TaggedRef::Bytes(b),
                Self::List(l) => TaggedRef::List(l),
                Self::Map(m) => TaggedRef::Map(m),
            }
            .serialize(s);
        }
        match self {
            Self::Null => s.serialize_none(),
            Self::Bool(b) => s.serialize_bool(*b),
            Self::Int(n) => match u64::try_from(*n) {
                Ok(n) => s.serialize_u64(n),
                Err(_) => s.serialize_i64(*n),
            },
            Self::Uint(n) => s.serialize_u64(*n),
            Self::Float(f) => finite_float::serialize(f, s),
            Self::Text(t) => s.serialize_str(t),
            Self::Bytes(b) => s.serialize_bytes(b),
            Self::List(l) => {
                let mut seq = s.serialize_seq(Some(l.len()))?;
                for v in l {
                    seq.serialize_element(v)?;
                }
                seq.end()
            }
            Self::Map(m) => {
                let mut map = s.serialize_map(Some(m.len()))?;
                for (k, v) in m {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        if d.is_human_readable() {
            return Ok(match Tagged::deserialize(d)? {
                Tagged::Null => Self::Null,
                Tagged::Bool(b) => Self::Bool(b),
                Tagged::Int(n) => Self::from(n),
                Tagged::Uint(n) => Self::Uint(n),
                Tagged::Float(f) => Self::Float(f),
                Tagged::Text(t) => Self::Text(t),
                Tagged::Bytes(b) => Self::Bytes(b),
                Tagged::List(l) => Self::List(l),
                Tagged::Map(m) => Self::Map(m),
            });
        }
        Native { depth: 1 }.deserialize(d)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Native {
    pub(crate) depth: usize,
}

impl Native {
    fn inner<E: de::Error>(self) -> Result<Self, E> {
        if self.depth >= MAX_DEPTH {
            return Err(E::custom("Geri Value nested too deep"));
        }
        Ok(Self {
            depth: self.depth + 1,
        })
    }
}

impl<'de> DeserializeSeed<'de> for Native {
    type Value = Value;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Value, D::Error> {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Native {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a Geri value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Value, E> {
        Ok(Value::from(v))
    }

    fn visit_i128<E: de::Error>(self, v: i128) -> Result<Value, E> {
        if let Ok(v) = u64::try_from(v) {
            return Ok(Value::Uint(v));
        }
        i64::try_from(v)
            .map(Value::Int)
            .map_err(|_| E::custom("Geri Int out of i64 range"))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Value, E> {
        Ok(Value::Uint(v))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Value, E> {
        if !v.is_finite() {
            return Err(E::custom("Geri Float must be finite"));
        }
        Ok(Value::Float(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Value, E> {
        Ok(Value::Text(v.into()))
    }

    fn visit_string<E>(self, v: String) -> Result<Value, E> {
        Ok(Value::Text(v))
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Value, E> {
        Ok(Value::Bytes(v.into()))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Value, E> {
        Ok(Value::Bytes(v))
    }

    fn visit_none<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Value, D::Error> {
        self.deserialize(d)
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let inner = self.inner()?;
        let mut list = Vec::new();
        while let Some(v) = seq.next_element_seed(inner)? {
            list.push(v);
        }
        Ok(Value::List(list))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        let inner = self.inner()?;
        let mut map = BTreeMap::new();
        while let Some(k) = access.next_key::<String>()? {
            let v = access.next_value_seed(inner)?;
            map.insert(k, v);
        }
        Ok(Value::Map(map))
    }
}

impl Value {
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Self::Map(m) => m.get(key),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match *self {
            Self::Int(v) => Some(v as f64),
            Self::Uint(v) => Some(v as f64),
            Self::Float(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match *self {
            Self::Uint(v) => Some(v),
            Self::Int(v) => u64::try_from(v).ok(),
            _ => None,
        }
    }
}

mod finite_float {
    use serde::{Deserialize, Deserializer, Serializer, de, ser};

    pub fn serialize<S: Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
        if !value.is_finite() {
            return Err(ser::Error::custom("Geri Float must be finite"));
        }
        serializer.serialize_f64(*value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
        let value = f64::deserialize(deserializer)?;
        if !value.is_finite() {
            return Err(de::Error::custom("Geri Float must be finite"));
        }
        Ok(value)
    }
}

mod bytes {
    use alloc::vec::Vec;
    use core::fmt;
    use serde::{Deserializer, Serializer, de};

    pub fn serialize<S: Serializer>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        struct Bytes;

        impl<'de> de::Visitor<'de> for Bytes {
            type Value = Vec<u8>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a byte buffer")
            }

            fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<Self::Value, E> {
                Ok(value.into())
            }

            fn visit_byte_buf<E: de::Error>(self, value: Vec<u8>) -> Result<Self::Value, E> {
                Ok(value)
            }

            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut value = Vec::new();
                while let Some(byte) = seq.next_element()? {
                    value.push(byte);
                }
                Ok(value)
            }
        }

        deserializer.deserialize_byte_buf(Bytes)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Plain(pub Value);

impl<'de> Deserialize<'de> for Plain {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Native { depth: 1 }.deserialize(d).map(Plain)
    }
}

impl Serialize for Plain {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        PlainRef(&self.0).serialize(s)
    }
}

struct PlainRef<'a>(&'a Value);

impl Serialize for PlainRef<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Value::List(l) => {
                let mut seq = s.serialize_seq(Some(l.len()))?;
                for v in l {
                    seq.serialize_element(&PlainRef(v))?;
                }
                seq.end()
            }
            Value::Map(m) => {
                let mut map = s.serialize_map(Some(m.len()))?;
                for (k, v) in m {
                    map.serialize_entry(k, &PlainRef(v))?;
                }
                map.end()
            }
            Value::Null => s.serialize_none(),
            Value::Bool(b) => s.serialize_bool(*b),
            Value::Int(n) => s.serialize_i64(*n),
            Value::Uint(n) => s.serialize_u64(*n),
            Value::Float(f) => finite_float::serialize(f, s),
            Value::Text(t) => s.serialize_str(t),
            Value::Bytes(b) => s.collect_seq(b.iter()),
        }
    }
}
