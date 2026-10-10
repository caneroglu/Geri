# Géri 🐺

> 🐺 ***Geri** ("the ravenous") is one of Odin's two wolves. Whatever you put on the table, he eats.*

- [A post about Géri.](https://can-eroglu.com/posts/geri/)
- [How i use it?](https://lab.can-eroglu.com)

[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg?logo=rust)](https://blog.rust-lang.org/2025/02/20/Rust-1.85.0.html)
[![no_std](https://img.shields.io/badge/no__std-compatible-success.svg)](geri-proto)
[![Last commit](https://img.shields.io/github/last-commit/caneroglu/Geri/main.svg)](https://github.com/caneroglu/Geri/commits/main)
[![Personal project](https://img.shields.io/badge/personal-project-lightgrey.svg)](#status)

[Türkçe](README.md) · **English**

I build prototypes on small hardware: a router, an ESP32-C3, a Blue Pill with 20 KB of RAM and no network chip.
I wanted all of them to talk to one place, the same way. Each one should say what it is,
send its data without losing any when the link drops, and take commands without running them twice.
Nothing I found did all of that and still fit, so I wrote Géri.

I made it for myself. It works for me, so I'm sharing it.

![Géri demo](demo/demo.gif)

*Left: a source sends records. Right: the hub's log. A replayed batch comes back `duplicate`, a skipped sequence comes back `gap`.*


## What it is

A protocol, plus the Rust crates to speak it.

- A **source** (any device) connects to a **hub**, declares itself once: its entities, value types, and the commands it accepts.
- The hub stores what the source sends, validates commands against the declaration, and draws a live HUD from it. The hub has no source-specific code.
- The hub is either one binary on your own machine (`geri-hubd`, one SQLite file), or it lives on Cloudflare.
  Mine is on Cloudflare, because I didn't want to pay for hosting and their free tier is generous enough for my whole setup.
  Thanks to [workers-rs](https://github.com/cloudflare/workers-rs) it's the same Rust crates there, compiled to WebAssembly,
  not a second implementation. (I did hit the daily free quota once. Fixed my queries 🙂)
  The Cloudflare part isn't public yet.

## The rules

These never change. Everything else (which road it takes, encrypted or not) is up to you.

1. **The device introduces itself.** Géri doesn't need to know it beforehand. Plug it in, it says hi.
2. **If Géri says "got it", it's on disk.** Not "probably", not "in memory somewhere".
3. **Every message gets the next number.** Nothing gets lost, nothing gets counted twice. Your graphs won't lie to you.
4. **A command runs at most once.** Your relay won't fire twice and burn the house down.
5. **The device always dials out.** No port forwarding, no fiddling with the modem.
6. **Géri can nudge, but the device always asks.** If a nudge gets lost on the way, the next question catches it.
7. **The road doesn't matter.** Cable, Wi-Fi, internet: same messages, different road.
8. **Safe by default.** Want to loosen something (say, ordering for throwaway readings)? Sure, but on purpose.

## Try it

A hub and a `curl` source, no SDK:

```sh
export GERI_AGENTS='[
  {"id":"sat","token":"a-long-random-agent-token","sources":["nasa-sat-01"]},
  {"id":"me","token":"a-long-random-operator-token","role":"operator"}
]'
cargo run --release -p geri-hubd   # http://127.0.0.1:8787
```

```sh
H=http://127.0.0.1:8787; AUTH='Authorization: Bearer a-long-random-agent-token'
curl -X PUT $H/v4/sources/nasa-sat-01 -H "$AUTH" -H 'content-type: application/json' -d @demo/source.json
curl $H/v4/batch -H "$AUTH" -H 'content-type: application/json' -d @demo/1.json
```

Open <http://127.0.0.1:8787/> and log in with the operator token to see the HUD.

A Rust source on Linux:

```rust
let (geri, mut events) = geri_sdk_linux::builder("https://hub.example", "", "router")
    .key("/etc/geri/key")              // Ed25519 instead of a token
    .source(declaration)               // what this source is and what it accepts
    .outbox("/etc/geri/outbox.sqlite") // nothing is lost while offline
    .start()?;

geri.state("router", "cpu", json!({ "t": "gauge", "v": 12.5, "max": 100, "unit": "%" }))?;
geri.live("router", live_values)?;     // sent only while someone is watching
// events: Command(..) -> do it -> geri.result(&command, ok, value)
```

## Transports and security

| | Push | Crate | I use it for |
| :--- | :---: | :--- | :--- |
| HTTP | piggyback + long-poll | `geri-transport-reqwest` | anything; `curl` is enough |
| WebSocket | yes | `geri-transport-ws` | the router → Cloudflare |
| TCP + Noise IK | yes | `geri-transport-tcp`, `geri-noise` | my own hub, no TLS, no CA |
| QUIC | yes | `geri-transport-quinn` | things that change IPs |
| Serial (COBS + CRC-32) | yes | `geri-transport-serial` | boards without a network, through a relay |

JSON or CBOR on every one. Identity is a token or an Ed25519 signature with a counter (no token on the wire, no clock needed).
The hub signs its log pages with Ed25519, so you can prove nothing was changed.

## Small hardware

That's the point of the whole thing, so here's where it stands:

- `geri-proto`, the bindings, the SDK core (`Session`), `geri-noise` and `geri-store-flash` are `no_std`.
- The flash outbox stores a typical record in ~36 bytes and survives power loss at every byte (tested).
- My router's node is a 4.3 MB static binary at ~4.6 MB RSS, with TLS, WebSocket, a SQLite outbox and signing.
- **Blue Pill doesn't fit yet.** The core still uses `alloc` and `serde_json::Value`; measured on thumbv7m it's ~110 KB.
  ESP32-C3, Pico W and the bigger STM32s are fine. A heapless core for the Blue Pill is next.

## Crates

| Crate | What |
| :--- | :--- |
| `geri-proto` | types, the message set, CBOR · `no_std` |
| `geri-core` | the rules: sequence/gap/duplicate, command checks, digests · sans-I/O |
| `geri-binding-http` / `-stream` / `-serial` | message set ⇄ HTTP, WS/byte streams, serial frames · `no_std` |
| `geri-adapter-sdk` | the `Session` state machine: outbox, batching, backoff, commands, live · `no_std` |
| `geri-sdk-linux` | Tokio driver, `builder()`, the `geri-key` tool |
| `geri-store-sqlite` / `-flash` | outboxes for Linux and NOR flash |
| `geri-transport-*` | reqwest, ws, tcp, quinn, serial |
| `geri-sign`, `geri-noise` | Ed25519 request signing, Noise IK · `no_std` |
| `geri-view` | the HUD renderer |
| `geri-hubd` | the reference hub |

Only `geri-proto` 0.1 is on crates.io. The rest goes up once there's documentation.

How it compares to MQTT, LwM2M, Zenoh & co, and what I've measured: [COMPARISON.md](COMPARISON.md).

## Status

Running at home every day: an OpenWrt router sends to a Cloudflare hub over a signed WebSocket.
Next, when I get to it: the heapless core for the Blue Pill, MCU SDKs, a CoAP binding.

It's a personal project. Issues are welcome, but I can't promise when I'll get to them.

## License

MIT or Apache-2.0, your choice ([LICENSE-MIT](LICENSE-MIT), [LICENSE-APACHE](LICENSE-APACHE)).
The frontend assets under `geri-view/assets/` keep their own licenses, see [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).
