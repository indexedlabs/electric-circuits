//! Sequencer transaction input: complete page runs stay in memory; incomplete runs spill past
//! their cap. With process admission enabled, held input and pages that need another byte owner
//! spill before that wait. Input streams back without restamping or rebuilding a transaction vector.
//! One awaited blocking operation per sequencer owns disk work; cancellation drops its returned
//! owner when the operation finishes. Spill files are scratch, never the recovery authority.

use std::collections::{HashSet, VecDeque};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::sync::{Arc, atomic::Ordering};

use anyhow::{Context, Result, ensure};

use crate::ds::{Envelope, envelope_memory_bytes};
use crate::metrics::Metrics;
use crate::txn_buffer::{HeldSpillFile, TxnBufferConfig};

const IO_BYTES: usize = 1024 * 1024;

pub(super) struct Transaction {
    pub key: (Option<String>, Option<String>),
    pub tables: HashSet<String>,
    pub last_seq: Option<u64>,
    pub complete: bool,
    pub count: usize,
    mem: VecDeque<Envelope>,
    mem_bytes: u64,
    spill: Option<Spilled>,
    max_envelope_bytes: u64,
    read_permit: crate::memory_budget::Permit,
    page_permit: Option<Arc<crate::memory_budget::Permit>>,
    // None for an already-complete run from a read page: it is not held input.
    cfg: Option<TxnBufferConfig>,
    metrics: Arc<Metrics>,
}

struct Spilled {
    file: HeldSpillFile,
    writer: Option<BufWriter<std::fs::File>>,
    reader: Option<BufReader<std::fs::File>>,
    read: usize,
    max_line_bytes: u64,
}

impl Transaction {
    pub fn held(cfg: TxnBufferConfig, metrics: Arc<Metrics>) -> Self {
        Self {
            key: (None, None),
            tables: HashSet::new(),
            last_seq: None,
            complete: false,
            count: 0,
            mem: VecDeque::new(),
            mem_bytes: 0,
            spill: None,
            max_envelope_bytes: 0,
            read_permit: crate::memory_budget::Permit::default(),
            page_permit: None,
            cfg: Some(cfg),
            metrics,
        }
    }

    pub fn page(envs: Vec<Envelope>, metrics: Arc<Metrics>) -> Self {
        let mut txn = Self::held(TxnBufferConfig::default(), metrics);
        txn.cfg = None;
        for env in envs {
            txn.note(&env);
            txn.mem.push_back(env);
        }
        txn
    }

    pub fn with_page_permit(mut self, permit: Option<Arc<crate::memory_budget::Permit>>) -> Self {
        self.page_permit = permit;
        self
    }

    fn note(&mut self, env: &Envelope) {
        self.max_envelope_bytes = self.max_envelope_bytes.max(envelope_memory_bytes(env));
        if self.count == 0 {
            self.key = super::sequencer::run_key_owned(env);
        }
        self.tables.insert(env.type_.clone());
        self.last_seq = env.headers.seq;
        self.complete = env.headers.last == Some(true) || env.headers.seq.is_none();
        self.count += 1;
    }

    /// Consumes the owner across the blocking boundary. Any write failure drops it and its file;
    /// the sequencer must stop at its pinned replay boundary, never retry a half-written scratch.
    pub async fn append(self, envs: Vec<Envelope>) -> Result<Self> {
        tokio::task::spawn_blocking(move || self.append_sync(envs)).await.context("joining held spill write")?
    }

    fn append_sync(mut self, envs: Vec<Envelope>) -> Result<Self> {
        let cfg = self.cfg.as_ref().context("appending to a complete page run")?.clone();
        for env in envs {
            self.note(&env);
            if let Some(spill) = &mut self.spill {
                spill.write(&env)?;
                continue;
            }
            let bytes = envelope_memory_bytes(&env);
            self.mem_bytes += bytes;
            self.metrics.sequencer_held_bytes.fetch_add(bytes, Ordering::Relaxed);
            self.mem.push_back(env);
            if self.mem_bytes > cfg.held_memory_bytes {
                let (file, handle) = HeldSpillFile::create(&cfg.spill_dir)?;
                let mut spill = Spilled {
                    file,
                    writer: Some(BufWriter::with_capacity(IO_BYTES, handle)),
                    reader: None,
                    read: 0,
                    max_line_bytes: 0,
                };
                while let Some(env) = self.mem.pop_front() {
                    spill.write(&env)?;
                }
                self.mem = VecDeque::new();
                self.metrics.sequencer_held_bytes.fetch_sub(self.mem_bytes, Ordering::Relaxed);
                self.mem_bytes = 0;
                self.metrics.sequencer_held_spills.fetch_add(1, Ordering::Relaxed);
                tracing::info!(txid = ?self.key.0, lsn = ?self.key.1, cap_bytes = cfg.held_memory_bytes,
                    path = %spill.file.path.display(), "sequencer held transaction spilled");
                self.spill = Some(spill);
            }
        }
        Ok(self)
    }

    /// The page reservation covers memory until this finishes. Spill even a complete page run
    /// before processing: its receipt may wait on a query-back that needs the same budget.
    pub async fn spool(self, cfg: TxnBufferConfig) -> Result<Self> {
        tokio::task::spawn_blocking(move || {
            let mut txn = self;
            if txn.spill.is_none() && !txn.mem.is_empty() {
                let (file, handle) = HeldSpillFile::create(&cfg.spill_dir)?;
                let mut spill = Spilled {
                    file,
                    writer: Some(BufWriter::with_capacity(IO_BYTES, handle)),
                    reader: None,
                    read: 0,
                    max_line_bytes: 0,
                };
                while let Some(env) = txn.mem.pop_front() {
                    spill.write(&env)?;
                }
                txn.mem = VecDeque::new();
                if txn.cfg.is_some() {
                    txn.metrics.sequencer_held_bytes.fetch_sub(txn.mem_bytes, Ordering::Relaxed);
                    txn.metrics.sequencer_held_spills.fetch_add(1, Ordering::Relaxed);
                }
                txn.mem_bytes = 0;
                txn.spill = Some(spill);
            }
            if let Some(spill) = &mut txn.spill
                && let Some(mut writer) = spill.writer.take()
            {
                writer.flush().context("flushing input page spill")?;
            }
            txn.page_permit = None;
            Ok(txn)
        })
        .await
        .context("joining input page spill")?
    }

    /// At most one bounded chunk (plus one indivisible envelope) is decoded at a time. The
    /// transaction owner keeps the file until its last append/receipt has finished processing.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<Envelope>>> {
        // Reserve the bounded reader workspace once, including its persistent BufReader.
        // Keep that owner across chunks; the terminal None releases before receipt waits.
        if let Some(spill) = &self.spill
            && self.read_permit.bytes() == 0
        {
            let bytes =
                self.max_envelope_bytes.saturating_add(spill.max_line_bytes).saturating_add(3 * IO_BYTES as u64);
            self.read_permit = self.metrics.memory_budget.acquire(bytes).await;
        }

        let budget = self.cfg.as_ref().map_or(IO_BYTES as u64, |cfg| cfg.held_memory_bytes.min(IO_BYTES as u64).max(1));
        if let Some(mut spill) = self.spill.take() {
            let expected = self.count;
            // The blocking job must own its reservation too: aborting the async waiter does
            // not stop a running disk operation or free the envelopes it is decoding.
            let permit = std::mem::take(&mut self.read_permit);
            let (spill, result, permit) = tokio::task::spawn_blocking(move || {
                let result = spill.next_chunk(budget, expected);
                (spill, result, permit)
            })
            .await
            .context("joining held spill read")?;
            self.spill = Some(spill);
            self.read_permit = permit;
            if result.as_ref().is_ok_and(Option::is_none) {
                self.read_permit = crate::memory_budget::Permit::default();
            }
            return result;
        }
        let mut out = Vec::new();
        let mut bytes = 0;
        while let Some(env) = self.mem.pop_front() {
            let size = envelope_memory_bytes(&env);
            bytes += size;
            if self.cfg.is_some() {
                self.mem_bytes -= size;
                self.metrics.sequencer_held_bytes.fetch_sub(size, Ordering::Relaxed);
            }
            out.push(env);
            if bytes >= budget {
                break;
            }
        }
        Ok((!out.is_empty()).then_some(out))
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.cfg.is_some() {
            self.metrics.sequencer_held_bytes.fetch_sub(self.mem_bytes, Ordering::Relaxed);
        }
    }
}

impl Spilled {
    fn write(&mut self, env: &Envelope) -> Result<()> {
        self.max_line_bytes = self.max_line_bytes.max(crate::txn_buffer::serialized_json_len(env)?.saturating_add(1));
        if self.writer.is_none() && self.reader.is_none() && self.read == 0 {
            let handle = std::fs::OpenOptions::new()
                .append(true)
                .open(&self.file.path)
                .context("reopening held spill for append")?;
            self.writer = Some(BufWriter::with_capacity(IO_BYTES, handle));
        }
        let writer = self.writer.as_mut().context("writing held spill after replay started")?;
        // Exactly the ingestor's NDJSON Envelope format, retaining all incoming stamps.
        serde_json::to_writer(&mut *writer, env).context("serializing held envelope")?;
        writer.write_all(b"\n").with_context(|| format!("writing held spill {}", self.file.path.display()))
    }

    fn next_chunk(&mut self, budget: u64, expected: usize) -> Result<Option<Vec<Envelope>>> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush().with_context(|| format!("flushing held spill {}", self.file.path.display()))?;
        }
        if self.reader.is_none() && self.read < expected {
            self.reader = Some(BufReader::with_capacity(
                IO_BYTES,
                std::fs::File::open(&self.file.path)
                    .with_context(|| format!("opening held spill {}", self.file.path.display()))?,
            ));
        }
        if self.read == expected && self.reader.is_none() {
            return Ok(None);
        }
        let reader = self.reader.as_mut().context("held spill has no reader")?;
        let mut out = Vec::new();
        let mut bytes = 0;
        loop {
            let mut line = Vec::new();
            let n = (&mut *reader)
                .take(self.max_line_bytes.saturating_add(1))
                .read_until(b'\n', &mut line)
                .context("reading held spill")?;
            ensure!(n as u64 <= self.max_line_bytes, "held spill line exceeded the written envelope size");
            if n == 0 {
                ensure!(self.read == expected, "held spill truncated: read {} of {expected} envelopes", self.read);
                break;
            }
            ensure!(line.last() == Some(&b'\n'), "held spill has a truncated envelope");
            ensure!(self.read < expected, "held spill has extra envelopes");
            let env: Envelope = serde_json::from_slice(&line).context("parsing held spill envelope")?;
            self.read += 1;
            bytes += envelope_memory_bytes(&env);
            out.push(env);
            if bytes >= budget {
                break;
            }
        }
        if out.is_empty() {
            self.reader = None;
        }
        Ok((!out.is_empty()).then_some(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ds::EnvelopeHeaders;
    use std::os::unix::fs::PermissionsExt;

    struct Scratch(std::path::PathBuf);
    impl Scratch {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!("circuits-held-unit-{}", uuid::Uuid::new_v4())))
        }
        fn txn(&self, cap: u64, metrics: &Arc<Metrics>) -> Transaction {
            Transaction::held(
                TxnBufferConfig { held_memory_bytes: cap, spill_dir: self.0.clone(), ..Default::default() },
                metrics.clone(),
            )
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn envelope(seq: u64) -> Envelope {
        Envelope {
            type_: "public.items".into(),
            key: seq.to_string(),
            value: Some(serde_json::json!({"id":seq, "body":"x".repeat(1024)})),
            old: Some(serde_json::json!({"id":seq, "body":"before"})),
            headers: EnvelopeHeaders {
                operation: "update".into(),
                txid: Some("42".into()),
                lsn: Some("0/20".into()),
                seq: Some(seq),
                last: (seq == 3).then_some(true),
                offset: Some(format!("original-{seq}")),
            },
        }
    }

    #[tokio::test]
    async fn memory_budget_scratch_job_keeps_page_credit_after_its_waiter_is_dropped() {
        let dir = Scratch::new();
        let budget = crate::memory_budget::MemoryBudget::new(Some(10));
        let permit = Arc::new(budget.acquire(20).await);
        let txn = dir.txn(0, &Arc::new(Metrics::default())).with_page_permit(Some(permit.clone()));
        let (started, arrived) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        let (done, terminal) = tokio::sync::oneshot::channel();
        let task = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            held.recv().unwrap();
            drop(txn);
            done.send(()).unwrap();
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), arrived).await.unwrap().unwrap();
        drop((task, permit));
        assert_eq!(budget.held.load(Ordering::Relaxed), 20, "running scratch work still owns the allocation");
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), terminal).await.unwrap().unwrap();
        assert_eq!(budget.held.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn held_threshold_and_byte_gauge_cover_below_equal_above_and_zero() {
        let dir = Scratch::new();
        let metrics = Arc::new(Metrics::default());
        let size = envelope_memory_bytes(&envelope(0));
        for (cap, spills) in [(size + 1, false), (size, false), (size - 1, true), (0, true)] {
            let txn = dir.txn(cap, &metrics).append(vec![envelope(0)]).await.unwrap();
            assert_eq!(txn.spill.is_some(), spills, "cap={cap}");
            assert_eq!(metrics.sequencer_held_bytes.load(Ordering::Relaxed), if spills { 0 } else { size });
            metrics.reset();
            assert_eq!(metrics.sequencer_held_bytes.load(Ordering::Relaxed), if spills { 0 } else { size });
            drop(txn);
            assert_eq!(metrics.sequencer_held_bytes.load(Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn held_ndjson_retains_every_stamp_and_replays_in_bounded_chunks() {
        let dir = Scratch::new();
        let metrics = Arc::new(Metrics::default());
        let expected: Vec<_> = (0..4).map(envelope).collect();
        let mut txn = dir.txn(512, &metrics).append(expected.clone()).await.unwrap();
        let spill = txn.spill.as_mut().unwrap();
        spill.writer.as_mut().unwrap().flush().unwrap();
        let path = spill.file.path.clone();
        assert_eq!(std::fs::metadata(&dir.0).unwrap().permissions().mode() & 0o777, 0o700);
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        let raw = std::fs::read_to_string(&path).unwrap();
        let decoded: Vec<Envelope> = raw.lines().map(|line| serde_json::from_str(line).unwrap()).collect();
        assert_eq!(serde_json::to_value(decoded).unwrap(), serde_json::to_value(&expected).unwrap());
        for env in expected {
            let chunk = txn.next_chunk().await.unwrap().unwrap();
            assert_eq!(chunk.len(), 1, "an oversized envelope is indivisible, not an unbounded transaction read");
            assert_eq!(serde_json::to_value(chunk).unwrap(), serde_json::json!([env]));
            assert!(path.exists(), "scratch stays owned until transaction publication finishes");
        }
        assert!(txn.next_chunk().await.unwrap().is_none());
        assert_eq!(metrics.sequencer_held_spills.load(Ordering::Relaxed), 1);
        drop(txn);
        assert!(!path.exists());
        assert_eq!(metrics.sequencer_held_bytes.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn held_failed_spill_creation_releases_accounted_memory() {
        let dir = Scratch::new();
        std::fs::write(&dir.0, b"not a directory").unwrap();
        let metrics = Arc::new(Metrics::default());
        let result = dir.txn(0, &metrics).append(vec![envelope(0)]).await;
        assert!(result.is_err());
        assert_eq!(metrics.sequencer_held_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.sequencer_held_spills.load(Ordering::Relaxed), 0);
        std::fs::remove_file(&dir.0).unwrap();
    }

    #[tokio::test]
    async fn held_corrupt_or_truncated_scratch_is_never_an_accepted_prefix() {
        let dir = Scratch::new();
        let metrics = Arc::new(Metrics::default());
        let line = serde_json::to_string(&envelope(0)).unwrap();
        for (body, message) in [
            (String::new(), "held spill truncated"),
            (line.clone(), "truncated envelope"),
            ("not-json\n".into(), "parsing held spill envelope"),
            (format!("{line}\n{line}\n"), "extra envelopes"),
        ] {
            let mut txn = dir.txn(0, &metrics).append(vec![envelope(0)]).await.unwrap();
            let spill = txn.spill.as_mut().unwrap();
            spill.writer.as_mut().unwrap().flush().unwrap();
            let path = spill.file.path.clone();
            std::fs::write(&path, body).unwrap();
            let error = loop {
                match txn.next_chunk().await {
                    Ok(Some(_)) => continue,
                    Ok(None) => panic!("corrupt scratch was accepted"),
                    Err(error) => break error,
                }
            };
            assert!(format!("{error:#}").contains(message), "{error:#}");
            drop(txn);
            assert!(!path.exists());
        }
    }
}
