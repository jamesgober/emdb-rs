// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! Kill-the-child crash tests. A child process (this test binary,
//! re-entered through `child_entry`) runs a write or compaction loop and is killed (`SIGKILL` / `TerminateProcess`) at a
//! varying point. The parent then reopens the database and checks
//! that every acknowledged write is present, every acknowledged
//! delete stays deleted, and the open never refuses a crash artifact
//! as corruption.
//!
//! Rounds per test default to a quick setting; set
//! `EMDB_CRASH_ROUNDS` to run more (the release checklist runs 60).

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use emdb::Emdb;

fn rounds(default: u32) -> u32 {
    std::env::var("EMDB_CRASH_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Dir(PathBuf);

impl Dir {
    fn new(label: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        let mut p = std::env::temp_dir();
        p.push(format!("emdb-crash-{label}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&p).expect("create test dir");
        Self(p)
    }

    fn files(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(&self.0)
            .expect("read dir")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Child entry point: does nothing unless `EMDB_CRASH_CHILD` is set.
#[test]
fn child_entry() {
    let Ok(mode) = std::env::var("EMDB_CRASH_CHILD") else {
        return;
    };
    let path = PathBuf::from(std::env::var("EMDB_CRASH_PATH").expect("path"));
    let start: u32 = std::env::var("EMDB_CRASH_START")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    match mode.as_str() {
        "writes" => child_writes(&path, start),
        "concurrent" => child_concurrent(&path, start),
        "compact" => child_compact(&path, start),
        other => panic!("unknown child mode {other}"),
    }
}

fn say(line: &str) {
    let out = std::io::stdout();
    let mut out = out.lock();
    writeln!(out, "{line}").expect("stdout");
    out.flush().expect("flush stdout");
}

fn child_writes(path: &Path, start: u32) {
    let db = Emdb::open(path).expect("child open");
    for i in start..u32::MAX {
        db.insert(format!("k{i}"), vec![b'v'; (i % 3000) as usize])
            .expect("insert");
        if i % 7 == 0 && i >= start + 3 {
            let _ = db.remove(format!("k{}", i - 3)).expect("remove");
            db.flush().expect("flush");
            say(&format!("DEL {}", i - 3));
        }
        db.flush().expect("flush");
        say(&format!("ACK {i}"));
    }
}

fn child_concurrent(path: &Path, start: u32) {
    let db = Emdb::open(path).expect("child open");
    let threads: Vec<_> = (0..4_u32)
        .map(|t| {
            let db = db.clone();
            std::thread::spawn(move || {
                for i in 0..u32::MAX / 8 {
                    let n = start + i * 4 + t;
                    db.insert(format!("c{n}"), vec![b'w'; ((n * 37) % 5000) as usize])
                        .expect("insert");
                    db.flush().expect("flush");
                    say(&format!("ACK {n}"));
                }
            })
        })
        .collect();
    for t in threads {
        t.join().expect("writer");
    }
}

fn child_compact(path: &Path, start: u32) {
    let db = Emdb::open(path).expect("child open");
    if db.is_empty().expect("len") {
        for i in 0..3000_u32 {
            db.insert(format!("k{i}"), vec![b'z'; 500]).expect("insert");
        }
        for i in 0..1500_u32 {
            let _ = db.remove(format!("k{i}")).expect("remove");
        }
        db.flush().expect("flush");
    }
    say("READY");
    let mut n = start;
    loop {
        db.compact().expect("compact");
        // Writes between compactions must survive the next one.
        db.insert(format!("post{}", n % 10), format!("{n}"))
            .expect("insert");
        db.flush().expect("flush");
        say(&format!("POST {n}"));
        n += 1;
    }
}

fn spawn_child(mode: &str, path: &Path, start: u32) -> Child {
    let exe = std::env::current_exe().expect("current exe");
    Command::new(exe)
        .args(["child_entry", "--exact", "--nocapture", "--test-threads=1"])
        .env("EMDB_CRASH_CHILD", mode)
        .env("EMDB_CRASH_PATH", path)
        .env("EMDB_CRASH_START", start.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn child")
}

/// The value after `marker` in a child output line. The test harness
/// prints the test name on the same line as the child's first output,
/// so the marker is searched anywhere in the line.
fn field<'a>(line: &'a str, marker: &str) -> Option<&'a str> {
    line.rsplit_once(marker).map(|(_, rest)| rest.trim())
}

/// Read child lines until `stop` returns true or the child exits.
fn read_until(
    child: &mut Child,
    mut stop: impl FnMut(&str) -> bool,
) -> BufReader<std::process::ChildStdout> {
    let stdout = child.stdout.take().expect("child stdout");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).expect("read child") == 0 || stop(line.trim()) {
            return reader;
        }
    }
}

fn kill(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn test_kill_during_writes_keeps_acknowledged_state() {
    if std::env::var("EMDB_CRASH_CHILD").is_ok() {
        return;
    }
    let dir = Dir::new("writes");
    let path = dir.0.join("db.emdb");
    let mut acked = BTreeSet::new();
    let mut deleted = BTreeSet::new();
    let mut next_start = 0_u32;
    for round in 0..rounds(10) {
        let mut child = spawn_child("writes", &path, next_start);
        let target = 30 + (round * 37) % 200;
        let mut seen = 0;
        let mut last_ack = 0;
        let _reader = read_until(&mut child, |line| {
            if let Some(i) = field(line, "ACK ") {
                let i: u32 = i.parse().expect("ack");
                let _ = acked.insert(i);
                last_ack = i;
                next_start = i + 1;
                seen += 1;
            } else if let Some(i) = field(line, "DEL ") {
                let _ = deleted.insert(i.parse::<u32>().expect("del"));
            }
            seen >= target
        });
        kill(child);
        // The child may have removed `k{m - 3}` for some `m > last_ack`
        // after the parent stopped reading; those keys are in an unknown
        // state, not lost.
        acked.retain(|&i| deleted.contains(&i) || (i + 3) % 7 != 0 || i + 3 <= last_ack);
        next_start += 10_000;
        let db = Emdb::open(&path)
            .unwrap_or_else(|e| panic!("round {round}: reopen failed: {e}; {:?}", dir.files()));
        for i in &acked {
            let present = db.get(format!("k{i}")).expect("get").is_some();
            if deleted.contains(i) {
                assert!(!present, "round {round}: deleted k{i} resurrected");
            } else {
                assert!(present, "round {round}: acknowledged k{i} lost");
            }
        }
    }
}

#[test]
fn test_kill_during_concurrent_writes_reopens_cleanly() {
    if std::env::var("EMDB_CRASH_CHILD").is_ok() {
        return;
    }
    let dir = Dir::new("concurrent");
    let path = dir.0.join("db.emdb");
    let mut acked = BTreeSet::new();
    let mut next_start = 0_u32;
    for round in 0..rounds(10) {
        let mut child = spawn_child("concurrent", &path, next_start);
        let target = 40 + (round * 53) % 300;
        let mut seen = 0;
        let _reader = read_until(&mut child, |line| {
            if let Some(n) = field(line, "ACK ") {
                let n: u32 = n.parse().expect("ack");
                let _ = acked.insert(n);
                seen += 1;
            }
            seen >= target
        });
        kill(child);
        next_start = acked.iter().next_back().copied().unwrap_or(0) + 100_000;
        let db = Emdb::open(&path).unwrap_or_else(|e| {
            panic!(
                "round {round}: crash artifact refused as corruption: {e}; {:?}",
                dir.files()
            )
        });
        for n in &acked {
            assert!(
                db.get(format!("c{n}")).expect("get").is_some(),
                "round {round}: acknowledged c{n} lost"
            );
        }
    }
}

#[test]
fn test_kill_during_compaction_keeps_live_set() {
    if std::env::var("EMDB_CRASH_CHILD").is_ok() {
        return;
    }
    let dir = Dir::new("compact");
    let path = dir.0.join("db.emdb");
    let mut posts: Vec<Option<u32>> = vec![None; 10];
    for round in 0..rounds(10) {
        let mut child = spawn_child("compact", &path, round * 1_000_000);
        let mut ready = false;
        let reader = read_until(&mut child, |line| {
            ready = ready || line.ends_with("READY");
            ready
        });
        assert!(ready, "round {round}: child died before READY");
        // Collect acknowledged post-compaction writes in the background
        // while the child keeps compacting.
        let collector = std::thread::spawn(move || {
            let mut acked = Vec::new();
            for line in reader.lines() {
                let Ok(line) = line else { break };
                if let Some(n) = field(&line, "POST ") {
                    acked.push(n.parse::<u32>().expect("post"));
                }
            }
            acked
        });
        std::thread::sleep(Duration::from_millis(5 + u64::from(round * 13 % 97)));
        kill(child);
        for n in collector.join().expect("collector") {
            posts[(n % 10) as usize] = Some(n);
        }
        let db = Emdb::open(&path)
            .unwrap_or_else(|e| panic!("round {round}: reopen failed: {e}; {:?}", dir.files()));
        let live: usize = (1500..3000_u32)
            .filter(|i| db.get(format!("k{i}")).expect("get").is_some())
            .count();
        assert_eq!(live, 1500, "round {round}: live set changed");
        for (slot, n) in posts.iter().enumerate() {
            if let Some(n) = n {
                // The child may have overwritten the slot after its last
                // acknowledgement reached us; never with an older value.
                let got = db.get(format!("post{slot}")).expect("get");
                let got: u32 = String::from_utf8(got.expect("post present"))
                    .expect("utf8")
                    .parse()
                    .expect("number");
                assert!(
                    got >= *n,
                    "round {round}: post{slot} went back from {n} to {got}"
                );
            }
        }
        let files = dir.files();
        drop(db);
        assert!(
            !files.iter().any(|f| f.contains(".corrupt-")),
            "round {round}: unexpected corrupt sidecar: {files:?}"
        );
    }
}
