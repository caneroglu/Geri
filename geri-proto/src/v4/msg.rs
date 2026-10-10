use super::{ApiError, Batch, Command, CommandResult, Receipt, SourceDeclaration, Welcome};
use alloc::string::String;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum ToHub {
    Hello {
        v: u16,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth: Option<String>,
    },
    Declare(SourceDeclaration),
    Batch(Batch),
    Live {
        source_id: String,
        values: serde_json::Map<String, serde_json::Value>,
    },
    Claim {
        source_id: String,
    },
    Result {
        source_id: String,
        #[serde(flatten)]
        result: CommandResult,
    },
    Ping,
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum ToSource {
    Welcome(Welcome),
    Receipt(Receipt),
    Command(Command),
    Idle,
    Ok {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        watched: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next_sequence: Option<u64>,
    },
    Error {
        status: u16,
        #[serde(flatten)]
        error: ApiError,
    },
    Pong,
    Pace {
        source_id: String,
        ms: u32,
    },
    Ring {
        source_id: String,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Frame<M> {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<u32>,
    #[serde(flatten)]
    pub msg: M,
}

#[cfg(test)]
mod tests {
    use super::super::{Limits, Record, RecordKind, VERSION};
    use super::*;
    use alloc::{vec, vec::Vec};
    use serde::de::DeserializeOwned;
    use serde_json::json;

    fn roundtrip<T: Serialize + DeserializeOwned + PartialEq + core::fmt::Debug>(frame: &T) {
        let json = serde_json::to_vec(frame).unwrap();
        assert_eq!(&serde_json::from_slice::<T>(&json).unwrap(), frame);
        let cbor = cbor(frame);
        assert_eq!(&minicbor_serde::from_slice::<T>(&cbor).unwrap(), frame);
    }

    fn cbor<T: Serialize>(value: &T) -> Vec<u8> {
        crate::cbor::to_vec(value).unwrap()
    }

    fn req<M>(id: u32, msg: M) -> Frame<M> {
        Frame { id: Some(id), msg }
    }

    fn signal<M>(msg: M) -> Frame<M> {
        Frame { id: None, msg }
    }

    #[test]
    fn every_up_message_round_trips_in_both_codecs() {
        let ups = vec![
            ToHub::Hello {
                v: VERSION,
                auth: None,
            },
            ToHub::Declare(SourceDeclaration {
                source_id: "router".into(),
                label: "Router".into(),
                kind: "openwrt".into(),
                entities: vec![],
            }),
            ToHub::Batch(Batch {
                v: VERSION,
                records: vec![Record {
                    source_id: "router".into(),
                    entity_id: "cpu".into(),
                    record_id: "router-1".into(),
                    sequence: 1,
                    kind: RecordKind::State,
                    event_time_ms: Some(123),
                    content_type: "application/json".into(),
                    data: Some(json!({"pct": 12.5})),
                    blob: None,
                    indexed: None,
                }],
            }),
            ToHub::Live {
                source_id: "router".into(),
                values: json!({"cpu": {"t": "gauge", "v": 37, "max": 100}})
                    .as_object()
                    .unwrap()
                    .clone(),
            },
            ToHub::Claim {
                source_id: "router".into(),
            },
            ToHub::Result {
                source_id: "router".into(),
                result: CommandResult {
                    command_id: "c1".into(),
                    ok: true,
                    result: json!({"rebooted": true}),
                },
            },
            ToHub::Ping,
        ];
        for (id, msg) in ups.into_iter().enumerate() {
            roundtrip(&req(id as u32, msg));
        }
    }

    #[test]
    fn every_down_message_round_trips_in_both_codecs() {
        let downs = vec![
            req(
                1,
                ToSource::Welcome(Welcome {
                    v: VERSION,
                    agent_id: "router".into(),
                    limits: Limits {
                        max_batch_records: 128,
                        max_inline_bytes: 65536,
                        max_body_bytes: 1 << 20,
                        max_blob_bytes: 1 << 24,
                        max_webhook_bytes: 65536,
                    },
                }),
            ),
            req(
                2,
                ToSource::Receipt(Receipt {
                    v: VERSION,
                    records: vec![],
                }),
            ),
            req(
                3,
                ToSource::Command(Command {
                    command_id: "c1".into(),
                    source_id: "router".into(),
                    entity_id: "wan".into(),
                    action: "press".into(),
                    args: json!(null),
                    issued_by: "can".into(),
                    issued_at_ms: 1,
                }),
            ),
            req(4, ToSource::Idle),
            req(
                5,
                ToSource::Ok {
                    watched: Some(true),
                    next_sequence: None,
                },
            ),
            req(
                6,
                ToSource::Ok {
                    watched: None,
                    next_sequence: Some(42),
                },
            ),
            req(
                7,
                ToSource::Error {
                    status: 410,
                    error: ApiError {
                        code: "cursor_expired".into(),
                        message: "cursor purged".into(),
                        earliest_cursor: Some(1001),
                        counter: None,
                    },
                },
            ),
            req(8, ToSource::Pong),
            signal(ToSource::Pace {
                source_id: "router".into(),
                ms: 1000,
            }),
            signal(ToSource::Ring {
                source_id: "router".into(),
            }),
        ];
        for frame in &downs {
            roundtrip(frame);
        }
    }

    #[test]
    fn wire_shape_is_flat_and_tagged() {
        let frame = req(
            7,
            ToHub::Claim {
                source_id: "router".into(),
            },
        );
        assert_eq!(
            serde_json::to_value(&frame).unwrap(),
            json!({"id": 7, "t": "claim", "source_id": "router"})
        );
        let batch = req(
            1,
            ToHub::Batch(Batch {
                v: VERSION,
                records: vec![],
            }),
        );
        assert_eq!(
            serde_json::to_value(&batch).unwrap(),
            json!({"id": 1, "t": "batch", "v": 4, "records": []})
        );
        let result = req(
            2,
            ToHub::Result {
                source_id: "router".into(),
                result: CommandResult {
                    command_id: "c1".into(),
                    ok: false,
                    result: json!(null),
                },
            },
        );
        assert_eq!(
            serde_json::to_value(&result).unwrap(),
            json!({"id": 2, "t": "result", "source_id": "router",
                   "command_id": "c1", "ok": false, "result": null})
        );
        let error = req(
            3,
            ToSource::Error {
                status: 422,
                error: ApiError {
                    code: "invalid".into(),
                    message: "bad id".into(),
                    earliest_cursor: None,
                    counter: None,
                },
            },
        );
        assert_eq!(
            serde_json::to_value(&error).unwrap(),
            json!({"id": 3, "t": "error", "status": 422, "code": "invalid", "message": "bad id"})
        );
        let pace = signal(ToSource::Pace {
            source_id: "router".into(),
            ms: 0,
        });
        assert_eq!(
            serde_json::to_value(&pace).unwrap(),
            json!({"t": "pace", "source_id": "router", "ms": 0})
        );
    }

    #[test]
    fn ping_and_pong_are_fixed_strings_for_hibernation_auto_response() {
        let ping: Frame<ToHub> = signal(ToHub::Ping);
        let pong: Frame<ToSource> = signal(ToSource::Pong);
        assert_eq!(serde_json::to_string(&ping).unwrap(), r#"{"t":"ping"}"#);
        assert_eq!(serde_json::to_string(&pong).unwrap(), r#"{"t":"pong"}"#);
    }

    #[test]
    fn unknown_messages_decode_instead_of_failing() {
        let to_hub: Frame<ToHub> =
            serde_json::from_str(r#"{"id":3,"t":"teleport","to":"mars"}"#).unwrap();
        assert_eq!(to_hub, req(3, ToHub::Unknown));
        let to_source: Frame<ToSource> =
            serde_json::from_str(r#"{"t":"resync","from":9}"#).unwrap();
        assert_eq!(to_source, signal(ToSource::Unknown));
    }

    #[test]
    fn malformed_frames_error_instead_of_panicking() {
        for bad in [
            &b""[..],
            b"{}",
            b"{\"t\":\"batch\"}",
            b"{\"id\":\"x\",\"t\":\"ping\"}",
            b"[1,2,3]",
        ] {
            assert!(
                serde_json::from_slice::<Frame<ToHub>>(bad).is_err(),
                "{bad:?}"
            );
        }
        let junk: Vec<u8> = (0..=255).collect();
        assert!(minicbor_serde::from_slice::<Frame<ToSource>>(&junk).is_err());
    }
}
