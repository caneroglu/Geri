// Testler Claude tarafından yazılmıştır.

#![no_std]

extern crate alloc;

use alloc::{
    borrow::ToOwned,
    format,
    string::{String, ToString},
    vec::Vec,
};
use geri_proto::cbor;
use geri_proto::v4::{
    ApiError, VERSION,
    msg::{ToHub, ToSource},
    valid_id,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub const SIGNALS: &str = "Geri-Signals";

pub const WAIT_PATH: &str = "/v4/signals";

pub const MAX_WAIT_MS: u32 = 25_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Get,
    Put,
    Post,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Put => "PUT",
            Self::Post => "POST",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "GET" => Some(Self::Get),
            "PUT" => Some(Self::Put),
            "POST" => Some(Self::Post),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Codec {
    #[default]
    Json,
    Cbor,
}

impl Codec {
    pub fn content_type(self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::Cbor => "application/cbor",
        }
    }

    pub fn from_content_type(content_type: Option<&str>) -> Self {
        match content_type {
            Some(ct) if ct.trim_start().starts_with("application/cbor") => Self::Cbor,
            _ => Self::Json,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub method: Method,
    pub path: String,
    pub codec: Codec,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub codec: Codec,
    pub body: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("this message has no HTTP form")]
    NoHttpForm,
    #[error("request is not part of the message set")]
    NotAMessage,
    #[error("missing or invalid identifier")]
    InvalidId,
    #[error("path and body name different sources")]
    PathMismatch,
    #[error("encode failed: {0}")]
    Encode(String),
    #[error("decode failed: {0}")]
    Decode(String),
}

pub mod client {
    use super::*;

    pub fn request(msg: &ToHub, codec: Codec) -> Result<Request, Error> {
        let (method, path, body) = match msg {
            ToHub::Hello { .. } => (Method::Get, "/v4/welcome".to_owned(), Vec::new()),
            ToHub::Declare(d) => (
                Method::Put,
                format!("/v4/sources/{}", id(&d.source_id)?),
                encode(d, codec)?,
            ),
            ToHub::Batch(b) => (Method::Post, "/v4/batch".to_owned(), encode(b, codec)?),
            ToHub::Live { source_id, values } => (
                Method::Put,
                format!("/v4/live/{}", id(source_id)?),
                encode(values, codec)?,
            ),
            ToHub::Claim { source_id } => (
                Method::Get,
                format!("/v4/commands/claim?source={}", id(source_id)?),
                Vec::new(),
            ),
            ToHub::Result { source_id, result } => (
                Method::Post,
                format!("/v4/commands/result?source={}", id(source_id)?),
                encode(result, codec)?,
            ),
            ToHub::Ping | ToHub::Unknown => return Err(Error::NoHttpForm),
        };
        Ok(Request {
            method,
            path,
            codec,
            body,
        })
    }

    pub fn response(
        sent: &ToHub,
        status: u16,
        content_type: Option<&str>,
        body: &[u8],
    ) -> Result<ToSource, Error> {
        let codec = Codec::from_content_type(content_type);
        if !(200..300).contains(&status) {
            let error = decode::<ApiError>(body, codec).unwrap_or_else(|_| ApiError {
                code: "unknown".to_owned(),
                message: String::from_utf8_lossy(body).chars().take(256).collect(),
                earliest_cursor: None,
                counter: None,
            });
            return Ok(ToSource::Error { status, error });
        }
        Ok(match sent {
            ToHub::Hello { .. } => ToSource::Welcome(decode(body, codec)?),
            ToHub::Declare(_) => ToSource::Ok {
                watched: None,
                next_sequence: reply(body, codec)?.next_sequence,
            },
            ToHub::Result { .. } => ToSource::Ok {
                watched: None,
                next_sequence: None,
            },
            ToHub::Batch(_) => ToSource::Receipt(decode(body, codec)?),
            ToHub::Live { .. } => ToSource::Ok {
                watched: Some(reply(body, codec)?.watched.unwrap_or(false)),
                next_sequence: None,
            },
            ToHub::Claim { .. } if status == 204 => ToSource::Idle,
            ToHub::Claim { .. } => ToSource::Command(decode(body, codec)?),
            ToHub::Ping | ToHub::Unknown => return Err(Error::NoHttpForm),
        })
    }

    pub fn wait(ms: u32) -> Request {
        Request {
            method: Method::Get,
            path: format!("{WAIT_PATH}?wait={}", ms.min(MAX_WAIT_MS)),
            codec: Codec::Json,
            body: Vec::new(),
        }
    }

    pub fn signals(header: Option<&str>) -> Vec<ToSource> {
        header
            .and_then(|h| serde_json::from_str::<Vec<ToSource>>(h).ok())
            .unwrap_or_default()
            .into_iter()
            .filter(|s| matches!(s, ToSource::Ring { .. } | ToSource::Pace { .. }))
            .collect()
    }
}

pub mod hub {
    use super::*;

    pub fn request(
        method: Method,
        path_and_query: &str,
        content_type: Option<&str>,
        body: &[u8],
    ) -> Result<ToHub, Error> {
        let codec = Codec::from_content_type(content_type);
        let (path, query) = path_and_query
            .split_once('?')
            .unwrap_or((path_and_query, ""));
        let source = || {
            query
                .split('&')
                .find_map(|pair| pair.strip_prefix("source="))
                .ok_or(Error::InvalidId)
                .and_then(id)
                .map(str::to_owned)
        };
        Ok(match (method, path) {
            (Method::Get, "/v4/welcome") => ToHub::Hello {
                v: VERSION,
                auth: None,
            },
            (Method::Post, "/v4/batch") => ToHub::Batch(decode(body, codec)?),
            (Method::Get, "/v4/commands/claim") => ToHub::Claim {
                source_id: source()?,
            },
            (Method::Post, "/v4/commands/result") => ToHub::Result {
                source_id: source()?,
                result: decode(body, codec)?,
            },
            (Method::Put, _) => {
                if let Some(source_id) = segment(path, "/v4/sources/") {
                    let declaration: geri_proto::v4::SourceDeclaration = decode(body, codec)?;
                    if declaration.source_id != id(source_id)? {
                        return Err(Error::PathMismatch);
                    }
                    ToHub::Declare(declaration)
                } else if let Some(source_id) = segment(path, "/v4/live/") {
                    ToHub::Live {
                        source_id: id(source_id)?.to_owned(),
                        values: decode(body, codec)?,
                    }
                } else {
                    return Err(Error::NotAMessage);
                }
            }
            _ => return Err(Error::NotAMessage),
        })
    }

    pub fn response(msg: &ToSource, codec: Codec) -> Result<Response, Error> {
        let (status, body) = match msg {
            ToSource::Welcome(w) => (200, encode(w, codec)?),
            ToSource::Receipt(r) => (200, encode(r, codec)?),
            ToSource::Command(c) => (200, encode(c, codec)?),
            ToSource::Idle
            | ToSource::Ok {
                watched: None,
                next_sequence: None,
            } => (204, Vec::new()),
            ToSource::Ok {
                watched,
                next_sequence,
            } => (
                200,
                encode(
                    &OkReply {
                        watched: *watched,
                        next_sequence: *next_sequence,
                    },
                    codec,
                )?,
            ),
            ToSource::Error { status, error } => (*status, encode(error, codec)?),
            ToSource::Pong | ToSource::Pace { .. } | ToSource::Ring { .. } | ToSource::Unknown => {
                return Err(Error::NoHttpForm);
            }
        };
        Ok(Response {
            status,
            codec,
            body,
        })
    }

    pub fn wait(method: Method, path_and_query: &str) -> Option<u32> {
        let (path, query) = path_and_query
            .split_once('?')
            .unwrap_or((path_and_query, ""));
        if method != Method::Get || path != WAIT_PATH {
            return None;
        }
        let ms = query
            .split('&')
            .find_map(|pair| pair.strip_prefix("wait="))
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(MAX_WAIT_MS);
        Some(ms.min(MAX_WAIT_MS))
    }

    pub fn signals(signals: &[ToSource]) -> Option<String> {
        if signals.is_empty() {
            return None;
        }
        serde_json::to_string(signals).ok()
    }

    fn segment<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
        path.strip_prefix(prefix).filter(|rest| !rest.contains('/'))
    }
}

#[derive(Default, Serialize, Deserialize)]
struct OkReply {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    watched: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    next_sequence: Option<u64>,
}

fn reply(body: &[u8], codec: Codec) -> Result<OkReply, Error> {
    if body.is_empty() {
        Ok(OkReply::default())
    } else {
        decode(body, codec)
    }
}

fn id(s: &str) -> Result<&str, Error> {
    if valid_id(s) {
        Ok(s)
    } else {
        Err(Error::InvalidId)
    }
}

fn encode<T: Serialize>(value: &T, codec: Codec) -> Result<Vec<u8>, Error> {
    match codec {
        Codec::Json => serde_json::to_vec(value).map_err(|e| Error::Encode(e.to_string())),
        Codec::Cbor => cbor::to_vec(value).map_err(|e| Error::Encode(e.to_string())),
    }
}

fn decode<T: DeserializeOwned>(body: &[u8], codec: Codec) -> Result<T, Error> {
    match codec {
        Codec::Json => serde_json::from_slice(body).map_err(|e| Error::Decode(e.to_string())),
        Codec::Cbor => cbor::from_slice(body).map_err(|e| Error::Decode(e.to_string())),
    }
}

#[cfg(test)]
mod tests;
