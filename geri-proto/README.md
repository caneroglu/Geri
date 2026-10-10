# geri-proto

Wire contract for [Géri](https://github.com/caneroglu/geri): a self-declaring telemetry, command, and tamper-evident event log protocol.

Any source (a satellite, a router or a Tetris emulator... anything) declares itself once and gets a durable log, validated commands, and a live HUD. This crate holds only the protocol types. It is `#![no_std]` (it needs only `alloc`) and does no I/O, so it runs on microcontrollers and servers alike.

- **v4** (`geri_proto::v4`): batch ingestion with durable receipts, content-addressed blobs, schema-validated at-most-once commands, and Ed25519-signed log digests.
- **v3** (`Up` / `Down`): the streaming form for long-lived connections.
- **Controls** (`Control`, `Shape`, `Danger`): how a source declares what can be changed, and how a hub validates commands against it.
- **Encodings:** every type serializes to JSON and CBOR with the same semantics.

```rust
use geri_proto::{Danger, Entity, Shape};

let dimmer = Entity::new("light")
    .label("Living room light")
    .control(Shape::range(0.0, 100.0), Danger::Safe);
```

The normative protocol description is [`SPEC.md`](https://github.com/caneroglu/geri/blob/main/SPEC.md).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT License](LICENSE-MIT) at your option.
