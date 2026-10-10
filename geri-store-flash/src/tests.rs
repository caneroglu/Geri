extern crate std;

use super::*;
use alloc::{rc::Rc, string::String, vec::Vec};
use core::cell::{Cell, RefCell};
use embedded_storage::nor_flash::{ErrorType, NorFlashError, NorFlashErrorKind, ReadNorFlash};
use geri_proto::v4::{BlobRef, RecordKind};
use serde_json::json;

const PAGE: usize = 1024;

#[derive(Debug, PartialEq)]
struct Cut;

impl NorFlashError for Cut {
    fn kind(&self) -> NorFlashErrorKind {
        NorFlashErrorKind::Other
    }
}

struct Ram {
    mem: Rc<RefCell<Vec<u8>>>,
    budget: Rc<Cell<Option<usize>>>,
}

impl Ram {
    fn memory(pages: usize) -> Rc<RefCell<Vec<u8>>> {
        Rc::new(RefCell::new(vec![0xff; pages * PAGE]))
    }

    fn on(mem: &Rc<RefCell<Vec<u8>>>, budget: &Rc<Cell<Option<usize>>>) -> Self {
        Self {
            mem: mem.clone(),
            budget: budget.clone(),
        }
    }

    fn spend(&self, n: usize) -> Result<(), usize> {
        match self.budget.get() {
            None => Ok(()),
            Some(left) if left >= n => {
                self.budget.set(Some(left - n));
                Ok(())
            }
            Some(left) => {
                self.budget.set(Some(0));
                Err(left)
            }
        }
    }
}

impl ErrorType for Ram {
    type Error = Cut;
}

impl ReadNorFlash for Ram {
    const READ_SIZE: usize = 1;

    fn read(&mut self, offset: u32, bytes: &mut [u8]) -> Result<(), Cut> {
        let o = offset as usize;
        bytes.copy_from_slice(&self.mem.borrow()[o..o + bytes.len()]);
        Ok(())
    }

    fn capacity(&self) -> usize {
        self.mem.borrow().len()
    }
}

impl NorFlash for Ram {
    const WRITE_SIZE: usize = 4;
    const ERASE_SIZE: usize = PAGE;

    fn erase(&mut self, from: u32, to: u32) -> Result<(), Cut> {
        let (from, to) = (from as usize, to as usize);
        let done = match self.spend(to - from) {
            Ok(()) => to - from,
            Err(part) => part,
        };
        self.mem.borrow_mut()[from..from + done].fill(0xff);
        if done < to - from { Err(Cut) } else { Ok(()) }
    }

    fn write(&mut self, offset: u32, bytes: &[u8]) -> Result<(), Cut> {
        let o = offset as usize;
        let done = match self.spend(bytes.len()) {
            Ok(()) => bytes.len(),
            Err(part) => part,
        };
        let mut mem = self.mem.borrow_mut();
        for (m, b) in mem[o..o + done].iter_mut().zip(bytes) {
            *m &= b;
        }
        if done < bytes.len() { Err(Cut) } else { Ok(()) }
    }
}

impl MultiwriteNorFlash for Ram {}

struct Device {
    queue: Rc<RefCell<Vec<u8>>>,
    seen: Rc<RefCell<Vec<u8>>>,
    budget: Rc<Cell<Option<usize>>>,
    window: u64,
    lazy: usize,
    names: usize,
}

const NAMES: usize = 4;

impl Device {
    fn new(queue_pages: usize, seen_pages: usize, window: u64) -> Self {
        Self {
            queue: Ram::memory(queue_pages),
            seen: Ram::memory(seen_pages),
            budget: Rc::new(Cell::new(None)),
            window,
            lazy: 0,
            names: NAMES,
        }
    }

    fn names(mut self, n: usize) -> Self {
        self.names = n;
        self
    }

    fn lazy(mut self, n: usize) -> Self {
        self.lazy = n;
        self
    }

    fn freeze(&self) {
        self.budget.set(Some(0));
    }

    fn boot(&self) -> FlashStore<Ram, Ram> {
        self.budget.set(None);
        self.open().unwrap()
    }

    fn open(&self) -> Result<FlashStore<Ram, Ram>, Error<Cut>> {
        FlashStore::open(
            Ram::on(&self.queue, &self.budget),
            Ram::on(&self.seen, &self.budget),
            Config {
                seen_window: self.window,
                lazy_records: self.lazy,
                max_names: self.names,
            },
        )
    }
}

fn draft(source: &str, record_id: Option<&str>) -> Draft {
    Draft {
        source_id: source.into(),
        entity_id: "e".into(),
        record_id: record_id.map(Into::into),
        kind: RecordKind::Event,
        event_time_ms: Some(7),
        content_type: "application/json".into(),
        data: Some(json!({"x": 1, "none": null})),
        indexed: None,
        blob: None,
        unordered: false,
    }
}

fn unordered(source: &str, record_id: &str) -> Draft {
    Draft {
        unordered: true,
        ..draft(source, Some(record_id))
    }
}

fn seqs(s: &mut FlashStore<Ram, Ram>) -> Vec<(String, u64, String)> {
    s.peek(usize::MAX)
        .unwrap()
        .into_iter()
        .map(|r| (r.source_id, r.sequence, r.record_id))
        .collect()
}

#[test]
fn keeps_the_store_contract() {
    let mut s = Device::new(8, 4, 16).boot();
    let a1 = s.append(draft("b", None), None).unwrap().unwrap();
    let a2 = s.append(draft("b", Some("x")), None).unwrap().unwrap();
    let c1 = s.append(draft("a", None), None).unwrap().unwrap();
    assert_eq!((a1.sequence, a1.record_id.as_str()), (1, "b-1"));
    assert_eq!((a2.sequence, a2.record_id.as_str()), (2, "x"));
    assert_eq!(c1.sequence, 1, "sequences are per source");
    assert_eq!(s.append(draft("b", Some("x")), None).unwrap(), None);
    assert_eq!(s.pending().unwrap(), 3);

    assert_eq!(
        s.peek(10).unwrap(),
        vec![a1.clone(), a2.clone(), c1.clone()]
    );
    assert_eq!(s.peek(1).unwrap(), vec![a1]);

    s.remove("b", "b-1").unwrap();
    s.remove("b", "b-1").unwrap();
    s.remove("b", "nope").unwrap();
    assert_eq!(s.pending().unwrap(), 2);
    s.remove("b", "x").unwrap();
    assert_eq!(s.append(draft("b", Some("x")), None).unwrap(), None);
    assert_eq!(
        s.append(draft("b", None), None).unwrap().unwrap().sequence,
        3
    );

    let mut blob = draft("feed", Some("r1"));
    blob.blob = Some(BlobRef {
        sha256: "abc".into(),
        size: 3,
    });
    assert!(matches!(s.append(blob, Some(b"xyz")), Err(Error::Blob)));
    assert_eq!(s.blob("abc").unwrap(), None);

    let before = s.append(draft("u", None), None).unwrap().unwrap();
    let u1 = s.append(unordered("u", "m1"), None).unwrap().unwrap();
    let u2 = s.append(unordered("u", "m2"), None).unwrap().unwrap();
    let after = s.append(draft("u", None), None).unwrap().unwrap();
    assert_eq!((u1.sequence, u1.record_id.as_str()), (0, "m1"));
    assert_eq!((before.sequence, after.sequence), (1, 2));
    assert_eq!(after.record_id, "u-2");
    assert_eq!(s.append(unordered("u", "m1"), None).unwrap(), None);
    assert_eq!(s.append(draft("u", Some("m1")), None).unwrap(), None);
    let peeked = s.peek(100).unwrap();
    assert_eq!(
        peeked[peeked.len() - 4..],
        [before.clone(), u1.clone(), u2.clone(), after.clone()]
    );
    s.remove("u", "m1").unwrap();
    assert!(s.peek(100).unwrap().contains(&u2));
    assert_eq!(
        s.append(unordered("u", "m1"), None)
            .unwrap()
            .unwrap()
            .sequence,
        0
    );
    s.remove("u", "u-1").unwrap();
    let named = s.append(draft("u", Some("n")), None).unwrap().unwrap();
    s.remove("u", &named.record_id).unwrap();
    assert_eq!(s.append(unordered("u", "n"), None).unwrap(), None);
}

#[test]
fn renumbers_only_what_the_hub_never_saw() {
    let mut s = Device::new(8, 8, 16).names(16).boot();
    s.append(draft("b", None), None).unwrap().unwrap();
    s.append(draft("b", Some("x")), None).unwrap().unwrap();
    assert_eq!(s.sync("b", Some(5001)).unwrap(), Some((1, 5001)));
    let moved: Vec<_> = s
        .peek(10)
        .unwrap()
        .into_iter()
        .map(|r| (r.sequence, r.record_id))
        .collect();
    assert_eq!(moved, [(5001, "b-5001".into()), (5002, "x".into())]);
    let next = s.append(draft("b", None), None).unwrap().unwrap();
    assert_eq!((next.sequence, next.record_id.as_str()), (5003, "b-5003"));
    assert_eq!(s.append(draft("b", Some("x")), None).unwrap(), None);
    assert_eq!(s.sync("b", Some(1)).unwrap(), None);
    assert_eq!(s.peek(1).unwrap()[0].sequence, 5001);

    assert_eq!(s.sync("c", Some(10)).unwrap(), Some((1, 10)));
    assert_eq!(
        s.append(draft("c", None), None).unwrap().unwrap().sequence,
        10
    );

    s.append(draft("d", None), None).unwrap().unwrap();
    assert_eq!(s.sync("d", None).unwrap(), None);
    assert_eq!(s.sync("d", Some(99)).unwrap(), None);
    assert_eq!(
        s.append(draft("d", None), None).unwrap().unwrap().sequence,
        2
    );

    s.append(draft("e", None), None).unwrap().unwrap();
    assert_eq!(s.sync("e", Some(1)).unwrap(), None);

    s.append(unordered("v", "k1"), None).unwrap().unwrap();
    s.append(draft("v", None), None).unwrap().unwrap();
    s.append(unordered("v", "k2"), None).unwrap().unwrap();
    assert_eq!(s.sync("v", Some(7)).unwrap(), Some((1, 7)));
    let of = |s: &mut FlashStore<Ram, Ram>, source: &str| -> Vec<(u64, String)> {
        seqs(s)
            .into_iter()
            .filter(|(src, ..)| src == source)
            .map(|(_, n, id)| (n, id))
            .collect()
    };
    assert_eq!(
        of(&mut s, "v"),
        [(0, "k1".into()), (7, "v-7".into()), (0, "k2".into())]
    );
    s.remove("v", "k1").unwrap();
    s.remove("v", "v-7").unwrap();
    s.remove("v", "k2").unwrap();
    assert!(of(&mut s, "v").is_empty());
    s.append(unordered("w", "z"), None).unwrap().unwrap();
    assert_eq!(s.sync("w", Some(9)).unwrap(), Some((1, 9)));
    assert_eq!(
        s.append(draft("w", None), None).unwrap().unwrap().sequence,
        9
    );
    for _ in 0..4 {
        s.append(draft("g", None), None).unwrap().unwrap();
    }
    s.remove("g", "g-1").unwrap();
    s.remove("g", "g-2").unwrap();
    assert_eq!(s.sync("g", Some(2)).unwrap(), Some((3, 2)));
    assert_eq!(of(&mut s, "g"), [(2, "g-2".into()), (3, "g-3".into())]);
    s.remove("g", "g-3").unwrap();
    s.remove("g", "g-2").unwrap();
    assert!(of(&mut s, "g").is_empty());
}

#[test]
fn unordered_records_survive_a_restart_without_settling_their_source() {
    let dev = Device::new(8, 4, 16);
    let mut s = dev.boot();
    let u = s.append(unordered("u", "m1"), None).unwrap().unwrap();
    s.append(draft("r", None), None).unwrap().unwrap();
    s.append(unordered("r", "m2"), None).unwrap().unwrap();
    s.remove("r", "r-1").unwrap();
    drop(s);
    let mut s = dev.boot();
    assert_eq!(s.pending().unwrap(), 2);
    assert_eq!(s.peek(1).unwrap(), vec![u]);
    assert_eq!(s.append(unordered("u", "m1"), None).unwrap(), None);
    assert_eq!(s.sync("u", Some(40)).unwrap(), Some((1, 40)));
    assert_eq!(s.sync("r", Some(70)).unwrap(), Some((1, 70)));
    s.remove("u", "m1").unwrap();
    s.remove("r", "m2").unwrap();
    assert_eq!(
        s.append(unordered("u", "m1"), None)
            .unwrap()
            .unwrap()
            .sequence,
        0
    );
}

#[test]
fn forgets_external_ids_outside_the_window() {
    let mut s = Device::new(8, 4, 3).boot();
    for id in ["a", "b", "c", "d"] {
        let r = s.append(draft("feed", Some(id)), None).unwrap().unwrap();
        s.remove("feed", &r.record_id).unwrap();
    }
    let again = s.append(draft("feed", Some("a")), None).unwrap().unwrap();
    s.remove("feed", &again.record_id).unwrap();
    assert_eq!(s.append(draft("feed", Some("c")), None).unwrap(), None);
    for _ in 0..3 {
        s.append(draft("feed", None), None).unwrap().unwrap();
    }
    assert!(s.append(draft("feed", Some("c")), None).unwrap().is_some());
    let pending = s.append(draft("feed", Some("p")), None).unwrap().unwrap();
    for _ in 0..5 {
        s.append(draft("feed", None), None).unwrap().unwrap();
    }
    assert_eq!(s.append(draft("feed", Some("p")), None).unwrap(), None);
    assert!(s.peek(100).unwrap().contains(&pending));
}

#[test]
fn everything_survives_a_restart() {
    let dev = Device::new(8, 4, 16);
    let mut s = dev.boot();
    s.append(draft("b", None), None).unwrap().unwrap();
    s.append(draft("b", Some("x")), None).unwrap().unwrap();
    s.sync("b", Some(41)).unwrap();
    s.append(draft("b", Some("y")), None).unwrap().unwrap();
    s.remove("b", "b-41").unwrap();
    let before = seqs(&mut s);
    drop(s);

    let mut s = dev.boot();
    assert_eq!(seqs(&mut s), before);
    assert_eq!(s.pending().unwrap(), 2);
    assert_eq!(s.sync("b", Some(7)).unwrap(), None);
    assert_eq!(
        s.append(draft("b", None), None).unwrap().unwrap().sequence,
        44
    );
    assert_eq!(s.append(draft("b", Some("x")), None).unwrap(), None);
    s.remove("b", "x").unwrap();
    drop(s);
    let mut s = dev.boot();
    assert_eq!(s.append(draft("b", Some("x")), None).unwrap(), None);
}

#[test]
fn a_mark_moves_numbers_atomically_and_goes_away_with_its_records() {
    let dev = Device::new(8, 4, 16);
    let mut s = dev.boot();
    s.append(draft("m", None), None).unwrap().unwrap();
    s.append(draft("m", None), None).unwrap().unwrap();
    assert_eq!(s.sync("m", Some(100)).unwrap(), Some((1, 100)));
    s.append(draft("m", None), None).unwrap().unwrap();
    drop(s);

    let mut s = dev.boot();
    let got: Vec<u64> = seqs(&mut s).into_iter().map(|(_, n, _)| n).collect();
    assert_eq!(got, [100, 101, 102]);
    assert_eq!(seqs(&mut s)[0].2, "m-100");
    assert_eq!(s.sync("m", Some(5)).unwrap(), None);
    for n in [100, 101, 102] {
        s.remove("m", &alloc::format!("m-{n}")).unwrap();
    }
    drop(s);

    let mut s = dev.boot();
    assert_eq!(s.pending().unwrap(), 0);
    let mut raw = 0;
    let mut iter = block_on(s.queue.iter()).unwrap();
    while block_on(iter.next(&mut s.buf)).unwrap().is_some() {
        raw += 1;
    }
    assert_eq!(raw, 0, "no leftover mark");
    s.append(draft("m", None), None).unwrap().unwrap();
    assert_eq!(s.sync("m", Some(103)).unwrap(), Some((1, 103)));
}

#[test]
fn a_full_outbox_refuses_instead_of_overwriting() {
    let mut s = Device::new(2, 4, 16).boot();
    let mut kept = 0;
    let err = loop {
        match s.append(draft("f", None), None) {
            Ok(_) => kept += 1,
            Err(e) => break e,
        }
    };
    assert!(matches!(err, Error::Full), "{err}");
    assert!(kept > 5);
    assert_eq!(s.peek(usize::MAX).unwrap().len(), kept);
    assert_eq!(s.peek(1).unwrap()[0].sequence, 1, "oldest is still there");
}

#[test]
fn a_seen_region_too_small_for_the_window_is_refused() {
    let cap = (seen_capacity(2 * PAGE, PAGE, 4) - 2 * NAMES) as u64;
    assert!(cap > 0);
    assert!(matches!(
        Device::new(4, 2, cap + 1).open(),
        Err(Error::Config(_))
    ));
    assert!(Device::new(4, 2, cap).open().is_ok());
}

#[test]
fn the_window_fits_even_with_the_longest_ids() {
    let cap = (seen_capacity(3 * PAGE, PAGE, 4) - 2 * NAMES) as u64;
    let mut s = Device::new(8, 3, cap).boot();
    for i in 0..cap * 4 {
        let id = alloc::format!("{i:064}");
        let r = s.append(draft("long", Some(&id)), None).unwrap().unwrap();
        s.remove("long", &r.record_id).unwrap();
    }
    let last = alloc::format!("{:064}", cap * 4 - 1);
    assert_eq!(s.append(draft("long", Some(&last)), None).unwrap(), None);
}

#[test]
fn ids_beyond_the_region_are_refused_loudly() {
    let cap = (seen_capacity(3 * PAGE, PAGE, 4) - 2 * NAMES) as u64;
    let mut s = Device::new(8, 3, cap).boot();
    let mut refused = None;
    'outer: for i in 0..cap {
        for src in ["p", "q"] {
            let id = alloc::format!("{i:064}");
            let r = s.append(draft(src, Some(&id)), None).unwrap().unwrap();
            if let Err(e) = s.remove(src, &r.record_id) {
                refused = Some((e, src, r.sequence));
                break 'outer;
            }
        }
    }
    let (err, src, seq) = refused.expect("region should fill up");
    assert!(matches!(err, Error::SeenFull), "{err}");
    assert!(seqs(&mut s).iter().any(|(s, n, _)| s == src && *n == seq));
}

#[test]
fn survives_a_power_cut_at_every_point() {
    type Acked = Vec<(String, Option<String>, u64)>;
    fn n_of(r: &Record) -> u64 {
        r.data.as_ref().and_then(|d| d["n"].as_u64()).unwrap()
    }
    fn scenario(
        s: &mut FlashStore<Ram, Ram>,
        acked: &mut Acked,
        removing: &mut Option<u64>,
    ) -> Result<(), Error<Cut>> {
        for n in 0..12u64 {
            let src = if n % 3 == 0 { "a" } else { "b" };
            let id = match n {
                n if n % 2 == 0 => Some(alloc::format!("id{}", n % 5)),
                n if n % 4 == 1 => Some(alloc::format!("u{n}")),
                _ => None,
            };
            let mut d = draft(src, id.as_deref());
            d.unordered = n % 4 == 1;
            d.data = Some(json!({ "n": n }));
            if s.append(d, None)?.is_some() {
                acked.push((src.into(), id, n));
            }
            if n == 4 {
                s.sync("b", Some(50))?;
                s.sync("a", None)?;
            }
            if n % 4 == 3
                && let Some(r) = s.peek(1)?.pop()
            {
                *removing = Some(n_of(&r));
                s.remove(&r.source_id, &r.record_id)?;
                acked.retain(|(.., m)| *m != n_of(&r));
                *removing = None;
            }
        }
        Ok(())
    }
    let dev = Device::new(6, 4, 16);
    let mut s = dev.boot();
    dev.budget.set(Some(usize::MAX));
    scenario(&mut s, &mut Vec::new(), &mut None).unwrap();
    let total = usize::MAX - dev.budget.get().unwrap();
    assert!(total > 300, "{total}");

    for cut in 0..total {
        let dev = Device::new(6, 4, 16);
        let mut s = dev.boot();
        dev.budget.set(Some(cut));
        let (mut acked, mut removing) = (Vec::new(), None);
        let _ = scenario(&mut s, &mut acked, &mut removing);
        drop(s);
        dev.budget.set(None);
        let mut s = dev
            .open()
            .unwrap_or_else(|e| panic!("cut at {cut}: reopen failed: {e}"));
        let now = s.peek(usize::MAX).unwrap();
        for (src, id, n) in &acked {
            if now.iter().any(|r| n_of(r) == *n) {
                continue;
            }
            assert_eq!(removing, Some(*n), "cut at {cut}: lost {src}/{n}: {now:?}");
            if let Some(id) = id.as_ref().filter(|id| !id.starts_with('u')) {
                assert_eq!(
                    s.append(draft(src, Some(id)), None).unwrap(),
                    None,
                    "cut at {cut}: {src}/{id} left the outbox but is not remembered"
                );
            }
        }
        for src in ["a", "b"] {
            let mine: Vec<&Record> = now.iter().filter(|r| r.source_id == src).collect();
            let numbered: Vec<u64> = mine.iter().map(|r| r.sequence).filter(|n| *n > 0).collect();
            assert!(
                numbered.windows(2).all(|w| w[0] < w[1]),
                "cut at {cut}: {src} order"
            );
            let mut ids: Vec<&str> = mine.iter().map(|r| r.record_id.as_str()).collect();
            let len = ids.len();
            ids.sort();
            ids.dedup();
            assert_eq!(ids.len(), len, "cut at {cut}: {src} duplicate ids");
        }
        s.append(draft("a", None), None).unwrap().unwrap();
    }
}

#[test]
fn a_session_runs_on_it() {
    use geri_adapter_sdk::{Config as SessionConfig, Session};
    let decl = geri_proto::v4::SourceDeclaration {
        source_id: "bluepill".into(),
        label: "b".into(),
        kind: "stm32".into(),
        entities: vec![],
    };
    let store = Device::new(8, 4, 16).boot();
    let mut s = Session::new(SessionConfig::new(vec![decl]), store).unwrap();
    s.state("bluepill", "temp", json!(21)).unwrap();
    s.state("bluepill", "temp", json!(22)).unwrap();
    assert_eq!(s.store_mut().pending().unwrap(), 2);
}

fn state(source: &str) -> Draft {
    Draft {
        kind: RecordKind::State,
        ..draft(source, None)
    }
}

fn pending(s: &mut FlashStore<Ram, Ram>) -> Vec<(String, u64, String)> {
    seqs(s)
}

#[test]
fn lazy_states_never_touch_flash_while_the_hub_keeps_up() {
    let dev = Device::new(8, 4, 16).lazy(4);
    let mut s = dev.boot();
    s.sync("s", None).unwrap();
    dev.freeze();
    for n in 1..=100 {
        let r = s.append(state("s"), None).unwrap().unwrap();
        assert_eq!(
            (r.sequence, r.record_id.as_str()),
            (n, &*alloc::format!("s-{n}"))
        );
        assert_eq!(s.peek(10).unwrap(), vec![r.clone()]);
        s.remove("s", &r.record_id).unwrap();
    }
    assert_eq!(s.pending().unwrap(), 0);
}

#[test]
fn events_external_ids_and_a_full_tail_go_to_flash_in_order() {
    let dev = Device::new(8, 5, 16).lazy(2).names(8);
    let mut s = dev.boot();
    s.append(state("a"), None).unwrap().unwrap();
    s.append(state("b"), None).unwrap().unwrap();
    s.append(draft("a", None), None).unwrap().unwrap();
    s.append(
        Draft {
            kind: RecordKind::State,
            ..draft("c", Some("x"))
        },
        None,
    )
    .unwrap()
    .unwrap();
    s.append(state("a"), None).unwrap().unwrap();
    s.append(state("d"), None).unwrap().unwrap();
    s.append(state("d"), None).unwrap().unwrap();
    s.append(state("d"), None).unwrap().unwrap();
    s.append(state("e"), None).unwrap().unwrap();
    let before = pending(&mut s);
    let order: Vec<_> = before.iter().map(|(s, n, _)| (s.as_str(), *n)).collect();
    assert_eq!(
        order,
        [
            ("a", 1),
            ("b", 1),
            ("a", 2),
            ("c", 1),
            ("a", 3),
            ("d", 1),
            ("d", 2),
            ("d", 3),
            ("e", 1)
        ]
    );
    drop(s);
    let mut s = dev.boot();
    assert_eq!(pending(&mut s), before[..8]);
}

#[test]
fn a_lost_tail_leaves_no_gap_and_no_conflict() {
    let dev = Device::new(8, 4, 16).lazy(4);
    let mut s = dev.boot();
    s.sync("s", None).unwrap();
    for _ in 0..3 {
        s.append(state("s"), None).unwrap().unwrap();
    }
    drop(s);

    let mut s = dev.boot();
    assert_eq!(s.pending().unwrap(), 0);
    s.append(state("s"), None).unwrap().unwrap();
    assert_eq!(s.sync("s", Some(4)).unwrap(), Some((1, 4)));
    assert_eq!(pending(&mut s), [("s".into(), 4, "s-4".into())]);

    let dev = Device::new(8, 4, 16).lazy(4);
    let mut s = dev.boot();
    s.append(state("s"), None).unwrap().unwrap();
    drop(s);
    let mut s = dev.boot();
    s.append(state("s"), None).unwrap().unwrap();
    assert_eq!(s.sync("s", Some(1)).unwrap(), None);
    assert_eq!(pending(&mut s), [("s".into(), 1, "s-1".into())]);
}

#[test]
fn sync_renumbers_the_tail_in_ram() {
    let dev = Device::new(8, 4, 16).lazy(4);
    let mut s = dev.boot();
    s.append(state("s"), None).unwrap().unwrap();
    s.append(state("s"), None).unwrap().unwrap();
    dev.freeze();
    assert_eq!(s.sync("s", Some(10)).unwrap(), Some((1, 10)));
    assert_eq!(
        pending(&mut s),
        [
            ("s".into(), 10, "s-10".into()),
            ("s".into(), 11, "s-11".into())
        ]
    );
    assert_eq!(s.append(state("s"), None).unwrap().unwrap().sequence, 12);
    s.remove("s", "s-10").unwrap();
    assert_eq!(s.pending().unwrap(), 2);
}

#[test]
fn a_session_runs_on_a_lazy_store() {
    use geri_adapter_sdk::{Config as SessionConfig, Session};
    let decl = geri_proto::v4::SourceDeclaration {
        source_id: "s".into(),
        label: "s".into(),
        kind: "stm32".into(),
        entities: vec![],
    };
    let dev = Device::new(8, 4, 16).lazy(4);
    let mut s = Session::new(SessionConfig::new(vec![decl]), dev.boot()).unwrap();
    s.state("s", "temp", json!(21)).unwrap();
    s.event("s", "button", None, None, json!(true)).unwrap();
    drop(s);
    assert_eq!(dev.boot().pending().unwrap(), 2);
}

#[test]
fn a_source_with_records_in_flash_writes_states_to_flash_too() {
    let dev = Device::new(8, 4, 16).lazy(4);
    let mut s = dev.boot();
    s.append(draft("s", None), None).unwrap().unwrap();
    s.append(state("s"), None).unwrap().unwrap();
    drop(s);
    let mut s = dev.boot();
    assert_eq!(
        pending(&mut s),
        [("s".into(), 1, "s-1".into()), ("s".into(), 2, "s-2".into())]
    );
}

fn first_item_len(s: &mut FlashStore<Ram, Ram>) -> usize {
    let mut buf = [0; PAGE];
    let mut iter = block_on(s.queue.iter()).unwrap();
    block_on(iter.next(&mut buf)).unwrap().unwrap().len()
}

#[test]
fn a_typical_state_is_under_thirty_bytes() {
    let mut s = Device::new(8, 4, 16).boot();
    s.append(
        Draft {
            kind: RecordKind::State,
            entity_id: "temperature".into(),
            event_time_ms: Some(1_790_000_000_000),
            data: Some(json!(21.5)),
            ..draft("kenjutsu-c3", None)
        },
        None,
    )
    .unwrap();
    let len = first_item_len(&mut s);
    assert!(len <= 28, "{len} bytes");
}

#[test]
fn every_field_survives_the_round_trip() {
    let odd = [
        Draft {
            kind: RecordKind::Raw,
            content_type: "application/json; profile=x".into(),
            indexed: Some(json!({"tag": [1, 2.5, "ü", null]})),
            event_time_ms: None,
            ..draft("a", Some("a-01"))
        },
        Draft {
            data: None,
            indexed: Some(json!(0)),
            ..draft("a", Some("a-3"))
        },
        Draft {
            data: Some(json!({"f": 1.0e300, "i": -1, "u": u64::MAX, "s": ""})),
            ..draft("b", None)
        },
    ];
    let dev = Device::new(8, 4, 16);
    let mut s = dev.boot();
    let written: Vec<_> = odd
        .into_iter()
        .map(|d| s.append(d, None).unwrap().unwrap())
        .collect();
    assert_eq!(s.peek(10).unwrap(), written);
    drop(s);
    assert_eq!(dev.boot().peek(10).unwrap(), written, "after a restart too");
}

#[test]
fn names_are_reused_once_no_record_needs_them() {
    let dev = Device::new(8, 4, 16);
    let mut s = dev.boot();
    for i in 0..NAMES - 1 {
        let src = alloc::format!("s{i}");
        let r = s.append(draft(&src, None), None).unwrap().unwrap();
        s.remove(&src, &r.record_id).unwrap();
    }
    let kept = s.append(draft("new", None), None).unwrap().unwrap();
    drop(s);
    let mut s = dev.boot();
    assert_eq!(s.peek(10).unwrap(), vec![kept]);

    for i in 0..NAMES - 2 {
        s.append(draft(&alloc::format!("w{i}"), None), None)
            .unwrap()
            .unwrap();
    }
    let err = s.append(draft("one-too-many", None), None).unwrap_err();
    assert!(matches!(err, Error::NamesFull), "{err}");
    assert_eq!(s.pending().unwrap(), NAMES - 1, "nothing lost");
}

#[test]
fn recycling_names_survives_a_power_cut_at_every_point() {
    fn n_of(r: &Record) -> u64 {
        r.data.as_ref().and_then(|d| d["n"].as_u64()).unwrap()
    }
    fn source_of(n: u64) -> String {
        alloc::format!("r{}", n % 6)
    }
    fn scenario(
        s: &mut FlashStore<Ram, Ram>,
        acked: &mut Vec<u64>,
        removing: &mut Option<u64>,
    ) -> Result<(), Error<Cut>> {
        for n in 0..12 {
            let mut d = draft(&source_of(n), None);
            d.data = Some(json!({ "n": n }));
            s.append(d, None)?;
            acked.push(n);
            if s.pending()? > 1 {
                let r = s.peek(1)?.remove(0);
                *removing = Some(n_of(&r));
                s.remove(&r.source_id, &r.record_id)?;
                acked.retain(|m| *m != n_of(&r));
                *removing = None;
            }
        }
        Ok(())
    }
    let dev = Device::new(6, 4, 16);
    let mut s = dev.boot();
    dev.budget.set(Some(usize::MAX));
    scenario(&mut s, &mut Vec::new(), &mut None).unwrap();
    let total = usize::MAX - dev.budget.get().unwrap();

    for cut in 0..total {
        let dev = Device::new(6, 4, 16);
        let mut s = dev.boot();
        dev.budget.set(Some(cut));
        let (mut acked, mut removing) = (Vec::new(), None);
        let _ = scenario(&mut s, &mut acked, &mut removing);
        drop(s);
        dev.budget.set(None);
        let mut s = dev
            .open()
            .unwrap_or_else(|e| panic!("cut at {cut}: reopen failed: {e}"));
        let now = s.peek(usize::MAX).unwrap();
        for r in &now {
            assert_eq!(r.source_id, source_of(n_of(r)), "cut at {cut}: wrong name");
        }
        for n in &acked {
            assert!(
                Some(*n) == removing || now.iter().any(|r| n_of(r) == *n),
                "cut at {cut}: lost {n}"
            );
        }
    }
}

#[test]
fn a_full_outbox_can_sync_and_drain_after_a_restart() {
    let dev = Device::new(2, 4, 16);
    let source = "x".repeat(43);
    let mut s = dev.boot();
    let mut count = 0;
    loop {
        match s.append(draft(&source, None), None) {
            Ok(Some(_)) => count += 1,
            Err(Error::Full) => break,
            other => panic!("{other:?}"),
        }
    }
    drop(s);
    let mut s = dev.boot();
    s.sync(&source, Some(41)).unwrap();
    drop(s);
    let mut s = dev.boot();
    let records = s.peek(usize::MAX).unwrap();
    assert_eq!(records.len(), count);
    assert_eq!(records[0].sequence, 41);
    for r in records {
        s.remove(&r.source_id, &r.record_id).unwrap();
    }
    assert_eq!(s.pending().unwrap(), 0);
    assert_eq!(
        s.append(draft(&source, None), None)
            .unwrap()
            .unwrap()
            .sequence,
        41 + count as u64
    );
}

#[test]
fn explicit_auto_shaped_ids_and_null_survive_sync_and_reboot() {
    let dev = Device::new(8, 4, 16);
    let mut s = dev.boot();
    s.append(
        Draft {
            data: Some(json!(null)),
            indexed: Some(json!(null)),
            ..draft("router", Some("router-1"))
        },
        None,
    )
    .unwrap();
    s.append(draft("router", None), None).unwrap();
    drop(s);
    let mut s = dev.boot();
    s.sync("router", Some(41)).unwrap();
    drop(s);
    let mut s = dev.boot();
    let records = s.peek(10).unwrap();
    assert_eq!(
        (&*records[0].record_id, records[0].sequence),
        ("router-1", 41)
    );
    assert_eq!(
        (&*records[1].record_id, records[1].sequence),
        ("router-42", 42)
    );
    assert_eq!(records[0].data, Some(json!(null)));
    assert_eq!(records[0].indexed, Some(json!(null)));
    s.remove("router", "router-1").unwrap();
    assert_eq!(
        s.append(draft("router", Some("router-1")), None).unwrap(),
        None
    );
}

#[test]
fn generated_id_collision_spends_no_sequence_or_flash() {
    let dev = Device::new(8, 4, 16);
    let mut s = dev.boot();
    let r = s
        .append(draft("router", Some("router-2")), None)
        .unwrap()
        .unwrap();
    dev.freeze();
    assert!(matches!(
        s.append(draft("router", None), None),
        Err(Error::IdCollision(_))
    ));
    assert_eq!(s.peek(10).unwrap(), [r]);
    dev.budget.set(None);
    assert_eq!(
        s.append(draft("router", Some("other")), None)
            .unwrap()
            .unwrap()
            .sequence,
        2
    );
}

#[test]
fn sync_identity_collision_leaves_flash_unchanged() {
    let dev = Device::new(8, 4, 16);
    let mut s = dev.boot();
    s.append(draft("router", Some("router-42")), None).unwrap();
    s.append(draft("router", None), None).unwrap();
    let before = s.peek(10).unwrap();
    let queue = dev.queue.borrow().clone();
    let seen = dev.seen.borrow().clone();
    assert!(matches!(
        s.sync("router", Some(41)),
        Err(Error::IdCollision(_))
    ));
    assert_eq!(s.peek(10).unwrap(), before);
    assert_eq!(*dev.queue.borrow(), queue);
    assert_eq!(*dev.seen.borrow(), seen);
    s.sync("router", Some(100)).unwrap();
    assert_eq!(s.peek(10).unwrap()[1].record_id, "router-101");
}

#[test]
fn sync_marks_keep_their_reserved_slots_when_seen_fills() {
    let dev = Device::new(8, 4, 12).names(6);
    let mut s = dev.boot();
    let mut seen_full = false;
    for n in 0..50 {
        let source = if n % 2 == 0 { "a" } else { "b" };
        let id = alloc::format!("id-{n}");
        let r = s.append(draft(source, Some(&id)), None).unwrap().unwrap();
        if matches!(s.remove(source, &r.record_id), Err(Error::SeenFull)) {
            seen_full = true;
            break;
        }
    }
    assert!(seen_full);
    s.sync("a", None).unwrap();
    s.sync("b", None).unwrap();
    drop(s);
    let mut s = dev.boot();
    assert!(s.pending().unwrap() > 0);
}
