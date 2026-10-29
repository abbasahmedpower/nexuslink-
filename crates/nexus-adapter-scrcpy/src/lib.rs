//! nexus-adapter-scrcpy — process supervision for the official scrcpy binary.
//!
//! P0 lifecycle fix (race-safe shutdown):
//! - The `Child` handle is NEVER stored inside a mutex that stop() must acquire,
//!   and no lock is EVER held across `child.wait().await`. Ownership lives in a
//!   dedicated reaper task; state changes flow through an `Arc<Session>` whose
//!   fields are updated with short-lived locks only.
//! - stop() communicates with the reaper through per-session control channels,
//!   so "monitor waiting on exit" and "stopper requesting termination" can never
//!   deadlock against each other.
//! - Bounded event channel with drop-newest policy: a stalled consumer cannot
//!   grow memory without bound.

use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{process::Child, sync::mpsc};

pub const EXIT_EVENT_CAP: usize = 256;
pub const DEFAULT_KILL_GRACE: Duration = Duration::from_millis(3000);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedKind {
    ScrcpyMirror,
    ScrcpyCamera,
    KdeCli,
    DiscoveryDaemon,
}

impl fmt::Display for ManagedKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ManagedKind::ScrcpyMirror => "scrcpy-mirror",
            ManagedKind::ScrcpyCamera => "scrcpy-camera",
            ManagedKind::KdeCli => "kde-cli",
            ManagedKind::DiscoveryDaemon => "discovery",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone)]
pub struct ExitEvent {
    pub handle: u64,
    pub kind: ManagedKind,
    /// Final exit status of the process, `None` if it was killed/unknown.
    pub status: Option<String>,
}

/// Shared session record. Cheap to clone (Arc), contains NO Child handle —
/// the child is owned exclusively by the reaper task (single-owner invariant).
struct Session {
    kind: ManagedKind,
    running: AtomicBool,
    /// Notified when the slot reaches its terminal state.
    done: tokio::sync::Notify,
}

enum Control {
    Terminate,
}

/// Thread-safe supervisor. Locks guard only O(1) bookkeeping maps; they are
/// never held across an await, so there is no deadlock surface at all.
pub struct Supervisor {
    inner: Mutex<Vec<(u64, Arc<Session>)>>,
    next_id: AtomicU64,
    tx: mpsc::Sender<ExitEvent>,
    rx: Arc<Mutex<Option<mpsc::Receiver<ExitEvent>>>>,
    controls: Mutex<Vec<(u64, mpsc::UnboundedSender<Control>)>>,
}

impl Supervisor {
    pub fn new() -> Arc<Self> {
        let (tx, rx) = mpsc::channel(EXIT_EVENT_CAP);
        Arc::new(Self {
            inner: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(1),
            tx,
            rx: Arc::new(Mutex::new(Some(rx))),
            controls: Mutex::new(Vec::new()),
        })
    }

    /// Take the event receiver (only the first caller gets it).
    pub fn subscribe(&self) -> Option<mpsc::Receiver<ExitEvent>> {
        self.rx.lock().unwrap().take()
    }

    /// Number of slots currently registered (running or mid-exit).
    pub fn count(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Snapshot of live managed processes.
    pub fn list(&self) -> Vec<(u64, ManagedKind)> {
        let g = self.inner.lock().unwrap();
        let mut v: Vec<_> = g
            .iter()
            .filter_map(|(k, s)| s.running.load(Ordering::Acquire).then_some((*k, s.kind)))
            .collect();
        v.sort_by_key(|(k, _)| *k);
        v
    }

    pub fn is_running(&self, kind: ManagedKind) -> bool {
        self.list().iter().any(|(_, k)| *k == kind)
    }

    /// True if a slot with this handle exists (running or mid-exit). Used by the
    /// application layer to distinguish "stopped" from "unknown/stale handle"
    /// instead of silently returning success for handles that never existed.
    pub fn exists(&self, handle: u64) -> bool {
        self.inner.lock().unwrap().iter().any(|(h, _)| *h == handle)
    }

    /// Adopt an already-spawned child under supervision. The child moves into a
    /// dedicated reaper task which owns it until exit — nothing else can touch it.
    pub fn adopt(self: &Arc<Self>, kind: ManagedKind, child: Child) -> u64 {
        let handle = self.next_id.fetch_add(1, Ordering::Relaxed);
        let session = Arc::new(Session {
            kind,
            running: AtomicBool::new(true),
            done: tokio::sync::Notify::new(),
        });
        self.inner.lock().unwrap().push((handle, session.clone()));
        let (ctl_tx, mut ctl_rx) = mpsc::unbounded_channel::<Control>();
        self.controls.lock().unwrap().push((handle, ctl_tx));

        let me = Arc::clone(self);
        tokio::spawn(async move {
            // ---- REAPER: sole owner of the Child. No lock is held here while waiting. ----
            let mut child = child;
            let status = tokio::select! {
                status = child.wait() => status.ok(),
                Some(Control::Terminate) = ctl_rx.recv() => {
                    #[cfg(unix)]
                    if let Some(pid) = child.id() {
                        send_sigterm(pid as i32);
                    }
                    // Graceful window, then hard kill. All awaits happen WITHOUT any lock.
                    if tokio::time::timeout(DEFAULT_KILL_GRACE, child.wait()).await.is_err() {
                        let _ = child.start_kill();
                        let _ = tokio::time::timeout(Duration::from_millis(1000), child.wait()).await;
                    }
                    None
                }
            };
            drop(child);
            session.running.store(false, Ordering::Release);
            session.done.notify_waiters();
            // Remove bookkeeping entries (short lock, no await inside).
            {
                let mut g = me.inner.lock().unwrap();
                g.retain(|(h, _)| *h != handle);
            }
            {
                let mut g = me.controls.lock().unwrap();
                g.retain(|(h, _)| *h != handle);
            }
            let event = ExitEvent {
                handle,
                kind,
                status: status.map(|s| s.to_string()),
            };
            // Bounded queue, drop-newest: the reaper must never block and memory must
            // never grow. Lost events are still observable via `list()`/state queries.
            if let Err(e) = me.tx.try_send(event) {
                tracing::warn!("exit-event queue full or closed, dropping event: {e}");
            }
        });
        handle
    }

    /// Request termination of a managed process and confirm it went terminal.
    /// Safe against races with natural exit: the reaper drains either path and
    /// we always finish by observing `done` or the removed slot.
    pub async fn stop(&self, handle: u64, grace: Duration) -> bool {
        // Register interest in the terminal notification BEFORE signalling, so a
        // fast exit cannot slip between notify and wait (classic lost-wakeup race).
        let session = self
            .inner
            .lock()
            .unwrap()
            .iter()
            .find(|(h, _)| *h == handle)
            .map(|(_, s)| s.clone());
        let Some(session) = session else { return true }; // unknown handle: already gone
        let notified = session.done.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let sender = self
            .controls
            .lock()
            .unwrap()
            .iter()
            .find(|(h, _)| *h == handle)
            .map(|(_, c)| c.clone());
        if let Some(c) = sender {
            let _ = c.send(Control::Terminate);
        }

        // Wait for the reaper to publish the terminal state (no lock held during await).
        if tokio::time::timeout(grace, notified).await.is_ok() {
            return true;
        }
        // Even on timeout we report best-effort truth: check whether the slot is gone.
        !self.inner.lock().unwrap().iter().any(|(h, _)| *h == handle)
    }

    /// Stop everything (shutdown path). Remaining children are terminated via
    /// their control channels; kill_on_drop is belt-and-suspenders.
    pub fn stop_all(&self) {
        let senders: Vec<_> = self
            .controls
            .lock()
            .unwrap()
            .iter()
            .map(|(_, c)| c.clone())
            .collect();
        for c in senders {
            let _ = c.send(Control::Terminate);
        }
    }
}

// --- tiny platform helper kept private to this module ---

#[cfg(unix)]
fn send_sigterm(pid: i32) {
    // SIGTERM delivered via libc crate (portable binding, checked return ignored:
    // ESRCH simply means the process already exited — fine for us).
    unsafe {
        libc::kill(pid, libc::SIGTERM);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::process::Command;

    fn spawn_dummy(secs: f64) -> Child {
        Command::new("sh")
            .arg("-c")
            .arg(format!("sleep {secs}"))
            .kill_on_drop(true)
            .spawn()
            .expect("spawn sh")
    }

    #[tokio::test]
    async fn reaps_exit_event() {
        let sup = Supervisor::new();
        let mut rx = sup.subscribe().expect("first subscriber");
        let h = sup.adopt(ManagedKind::KdeCli, spawn_dummy(0.05));
        let ev = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("event")
            .expect("channel open");
        assert_eq!(ev.handle, h);
        assert!(ev.status.is_some());
        assert_eq!(sup.count(), 0);
    }

    #[tokio::test]
    async fn graceful_stop_confirmed() {
        let sup = Supervisor::new();
        let _rx = sup.subscribe();
        let h = sup.adopt(ManagedKind::ScrcpyMirror, spawn_dummy(30.0));
        assert!(sup.is_running(ManagedKind::ScrcpyMirror));
        let ok = sup.stop(h, Duration::from_millis(800)).await;
        assert!(ok);
        assert_eq!(sup.count(), 0);
    }

    #[tokio::test]
    async fn stop_unknown_handle_is_true() {
        let sup = Supervisor::new();
        assert!(sup.stop(9999, Duration::from_millis(50)).await);
    }

    #[tokio::test]
    async fn stop_escalates_when_sigterm_is_ignored() {
        // Child traps SIGTERM and ignores it; supervisor escalates to SIGKILL.
        let sup = Supervisor::new();
        let _rx = sup.subscribe();
        let child = Command::new("sh")
            .arg("-c")
            .arg("trap '' TERM; while true; do sleep 0.1; done")
            .kill_on_drop(true)
            .spawn()
            .expect("spawn sh");
        let h = sup.adopt(ManagedKind::ScrcpyMirror, child);
        let ok = sup.stop(h, Duration::from_millis(300)).await;
        assert!(ok, "escalation should confirm termination");
        assert_eq!(sup.count(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn external_kill_updates_state() {
        // Micro-level scenario: process dies outside our control (adb crash etc.).
        let sup = Supervisor::new();
        let mut rx = sup.subscribe().expect("first subscriber");
        let child = Command::new("sh")
            .arg("-c")
            .arg("sleep 30")
            .kill_on_drop(true)
            .spawn()
            .expect("spawn");
        let pid = child.id().expect("pid");
        let h = sup.adopt(ManagedKind::ScrcpyMirror, child);
        unsafe { libc::kill(pid as i32, libc::SIGKILL) };
        let ev = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("event")
            .expect("open");
        assert_eq!(ev.handle, h);
        assert!(!sup.is_running(ManagedKind::ScrcpyMirror));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_stop_and_exit_no_deadlock_or_leak() {
        let sup = Supervisor::new();
        let _rx = sup.subscribe();
        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = sup.clone();
            handles.push(tokio::spawn(async move {
                let h = s.adopt(ManagedKind::KdeCli, spawn_dummy(0.02));
                // Race a stop against the natural exit; either outcome is fine.
                let _ = s.stop(h, Duration::from_millis(500)).await;
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(sup.count(), 0, "slots leaked");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stop_while_monitor_waits_never_deadlocks() {
        // The exact P0 regression: long-running child + immediate stop must complete.
        // Deterministic timing rationale: SIGTERM terminates `sh` almost instantly, but
        // its `sleep 60` grandchild inherits the stdout/stderr pipes and keeps them open,
        // so tokio's Child::wait (pipe-draining) only returns once the holder dies as well.
        // The grace window must comfortably exceed that drain latency on loaded CI machines;
        // the hard outer timeout still proves there is no deadlock (the original bug).
        let sup = Supervisor::new();
        let _rx = sup.subscribe();
        let h = sup.adopt(ManagedKind::ScrcpyMirror, spawn_dummy(60.0));
        // Grace (2s) > pipe-drain latency; outer timeout (5s) still bounds the test.
        let ok = tokio::time::timeout(
            Duration::from_secs(5),
            sup.stop(h, Duration::from_millis(2000)),
        )
        .await;
        assert!(ok.is_ok(), "stop deadlocked");
        assert!(ok.unwrap());
        assert_eq!(sup.count(), 0);
    }
}
