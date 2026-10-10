# Géri vs others

Longer notes behind the [README](README.en.md). What I measured, and how Géri lines up against what's already out there.

## What I measured

On my own setup: an OpenWrt router (Xiaomi AX3000T) feeding a Cloudflare Durable Object hub, plus a local `geri-hubd`.
No lab, no fancy rig. A proper Linux benchmark against the stacks below is still on my list.

Command latency (operator clicks → the source runs it):

```
+----------------------------------+----------+---------------------------------+
| path                             | latency  | note                            |
+==================================+==========+=================================+
| edge hub (Cloudflare), WebSocket | 31-39 ms | was 4.5 s avg with 10 s polling |
| local hubd, WebSocket            | ~2.5 ms  |                                 |
| local hubd, plain TCP            | ~2.5 ms  |                                 |
| local hubd, TCP + Noise IK       | ~3.5 ms  |                                 |
| local hubd, QUIC                 | ~3 ms    |                                 |
| local hubd, HTTP long-poll       | ~4 ms    | was up to 10 s polling          |
+----------------------------------+----------+---------------------------------+
```

Size and footprint:

```
+-------------------------------------------+--------------+-------------------------------------------------+
| what                                      | number       | note                                            |
+===========================================+==============+=================================================+
| router node binary (aarch64-musl, static) | 4.3 MB       | TLS, WS, SQLite outbox, signing, local HUD      |
| router node RSS                           | ~4.6 MB      | Glance (just a dashboard) used 20-25 MB there   |
| router idle requests per day              | ~0           | one sleeping WebSocket; was ~17,000             |
| flash outbox, typical state record        | ~36 B        | was ~195 B; survives power loss at every byte   |
| MCU core: Session + flash store           | ~110 KB      | thumbv7m, opt-level s, LTO; Blue Pill has 64 KB |
| geri-hubd idle / after 20k records        | 14 / 17 MB   | x86_64 Windows release build                    |
| geri-hubd on disk                         | 8.2 MB       | every transport included                        |
| geri-hubd ingest                          | ~7,000 rec/s | batches of 10, one connection, Windows          |
+-------------------------------------------+--------------+-------------------------------------------------+
```

## How it compares

My assessment, October 2026. If I got something wrong, open an issue and I'll fix it.
Géri isn't the first to do any single row. LwM2M/OSCORE already does end-to-end security through relays,
Zenoh matches it on transports, ESPHome also encrypts with Noise. What I wanted was all of it in one protocol
that also fits on small boards and draws its own UI.

### What the rows mean

In plain words, no jargon contest:

- **Sources describe themselves:** a device says "I'm a thermometer, and I have a switch" when it connects. Nobody types that into the hub by hand.
- **UI builds itself:** from that description, the hub draws a panel. A number becomes a gauge, a switch becomes a button.
- **Commands checked before sending:** if the switch only knows on/off, the hub won't let you send it "banana".
- **At-most-once + tracked result:** a command runs once or not at all, never twice. Then the device tells you how it went.
  Matters a lot when the command is "open the door".
- **Gap-free sequence:** every message from a device gets the next number: 1, 2, 3... If 4 is missing, the hub notices.
- **Durable receipt:** the hub only says "got it" after the data is safely on disk, not just in memory.
- **Offline outbox:** when the internet drops, the device keeps unsent data in a queue and sends it when the link is back.
- **Replays never duplicate:** if the device sends the same thing twice (it does, after a timeout), it's stored once.
- **One log:** readings (state), things that happened (events) and files (blobs) all go into the same history.
- **Proof the log is untouched:** the hub signs its history. If someone edits it later, the signature breaks and you can tell.
- **Same messages on every transport:** cable, Wi-Fi, internet; the words are the same, only the road changes.
- **Push, truth in pull:** the hub can nudge a device ("hey, you've got a command"), but the device always asks to be sure.
  If a nudge gets lost, nothing breaks, it just takes a bit longer.
- **Signed requests, no bearer:** instead of sending a password with every message (which anyone in the middle could copy),
  the device signs each message with a key that never leaves it.
- **Encryption without a CA:** traffic is encrypted without buying or managing certificates. Both sides just know each other's key.
- **End-to-end through relays:** a tiny board talks through a bigger board to reach the hub, and the middle one can't read or fake anything.
- **Plain curl can be a source:** no SDK needed; a shell one-liner can send data.
- **no_std:** the code runs on bare microcontrollers with no operating system.

### The tables

What it promises:

```
+---------------------------------+------+------------+----------+----------+----------+----------+
|                                 | Geri | ESPHome+HA | MQTT+SpB | LwM2M    | Zenoh    | DDS      |
+=================================+======+============+==========+==========+==========+==========+
| sources describe themselves     | yes  | yes        | yes      | part [1] | no       | yes      |
| UI builds itself                | yes  | yes        | no       | no       | no       | no       |
| commands checked before sending | yes  | yes        | part     | yes      | no       | part     |
| at-most-once + tracked result   | yes  | no         | part [2] | part     | no       | no       |
| gap-free seq + durable receipt  | yes  | no         | part [3] | no       | no       | part [4] |
| offline outbox, nothing lost    | yes  | no         | part [5] | no       | no       | part [6] |
| replays never duplicate         | yes  | no         | part     | part [7] | no       | part     |
| one log: state, events, blobs   | yes  | part       | no       | no       | part [8] | part     |
| proof the log is untouched      | yes  | no         | no       | no       | no       | no       |
+---------------------------------+------+------------+----------+----------+----------+----------+
```

How it travels:

```
+----------------------------+----------+------------+----------+----------+----------+-----------+
|                            | Geri     | ESPHome+HA | MQTT+SpB | LwM2M    | Zenoh    | DDS       |
+============================+==========+============+==========+==========+==========+===========+
| same messages on every one | yes      | -          | no [9]   | part     | yes      | part      |
| push, truth in pull        | yes      | part       | part     | yes [10] | part     | part      |
| signed requests, no bearer | yes      | no         | no       | yes [11] | no       | yes [12]  |
| encryption without a CA    | yes [13] | yes [14]   | no       | yes [15] | part     | no        |
| end-to-end through relays  | wip      | no         | no       | yes [11] | no       | no        |
| plain curl can be a source | yes      | no         | no       | no       | no       | no        |
| no_std core for MCUs       | yes [16] | yes [17]   | yes      | yes      | yes [18] | part [19] |
+----------------------------+----------+------------+----------+----------+----------+-----------+
```

Transports: Géri: HTTP, WS, TCP, QUIC, serial. ESPHome: TCP. MQTT: TCP, WS. LwM2M: UDP, TCP, SMS.
Zenoh: TCP, UDP, QUIC, WS, serial. DDS: UDP, TCP.


`part` = partly, or only with extra setup. `wip` = I'm working on it. SpB = Sparkplug B.

[1] registry IDs  
[2] QoS0, no result  
[3] seq, no receipt  
[4] in-session  
[5] optional  
[6] durability QoS  
[7] short window  
[8] storage plugin  
[9] MQTT-SN differs  
[10] Observe  
[11] OSCORE  
[12] PKI  
[13] Noise IK  
[14] Noise PSK  
[15] DTLS-RPK  
[16] SDK wip  
[17] ESP only  
[18] zenoh-pico  
[19] XRCE

The usual alternative stack is an MQTT broker, Telegraf or Node-RED, InfluxDB or Prometheus, Grafana,
and a hand-written API for commands: five services and five configs. Géri's hub is one binary and one SQLite file.
I haven't measured those stacks myself, so no numbers for them here.
