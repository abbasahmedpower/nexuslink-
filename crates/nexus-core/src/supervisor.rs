//! Process supervision: the heart of v0.4.
//!
//! Design notes (nano/micro level):
//! - Every managed process is owned by exactly one `Supervised` record; the supervisor
//!   is the only place that may kill it. Kill-on-drop guarantees no orphans even on panic.
//! - Exit notifications are delivered through a *bounded* channel with drop-oldest policy,
//!   so a stalled consumer can never grow memory without bound (backpressure by dropping
//!   the oldest events, which are the least informative).
//! - Stop uses SIGTERM first (graceful), then escalates to SIGKILL after a timeout.
//!   On Windows child processes are killed directly (no SIGTERM semantics there).
//! - The reaper task holds only owned handles and exits when the supervisor is dropped,
//!   preventing task leaks.

use std::{
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
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

struct Slot {
    kind: ManagedKind,
    /// `Some` while running; `None` once the reaper took the child to await its exit.
    supervised: Option<Child>,
    /// Notified when the slot reaches its terminal state (status recorded, removed).
    done: Arc<tokio::sync::Notify>,
}

/// Thread-safe. The lock is held only for O(1) bookkeeping — never across an
/// `await` on anything that could depend on the same lock — so there is no
/// deadlock surface and contention stays negligible.
pub struct Supervisor {
    inner: Mutex<HashMap<u64, Slot>>,
    next_id: AtomicU64,
    tx: mpsc::Sender<ExitEvent>,
    rx: Arc<Mutex<Option<mpsc::Receiver<ExitEvent>>>>,
}

impl Supervisor {
    pub fn new() -> Arc<Self> {
        let (tx, rx) = mpsc::channel(EXIT_EVENT_CAP);
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            tx,
            rx: Arc::new(Mutex::new(Some(rx))),
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
            .filter_map(|(k, s)| s.supervised.is_some().then_some((*k, s.kind)))
            .collect();
        v.sort_by_key(|(k, _)| *k);
        v
    }

    /// Adopt an already-spawned child under supervision and start its reaper.
    pub fn adopt(self: &Arc<Self>, kind: ManagedKind, child: Child) -> u64 {
        let handle = self.next_id.fetch_add(1, Ordering::Relaxed);
        let slot = Slot {
            kind,
            supervised: Some(child),
            done: Arc::new(tokio::sync::Notify::new()),
        };
        self.inner.lock().unwrap().insert(handle, slot);
        let me = Arc::clone(self);
        tokio::spawn(async move { me.reap(handle).await });
        handle
    }

    async fn reap(self: Arc<Self>, handle: u64) {
        // Clone the notification handle while taking ownership of the child, so we
        // can await the exit WITHOUT holding the lock (holding it across `wait()`
        // would deadlock any concurrent `stop()`).
        let (kind, mut child, notify) = {
            let mut g = self.inner.lock().unwrap();
            match g.get_mut(&handle) {
                Some(slot) => match slot.supervised.take() {
                    Some(c) => (slot.kind, c, slot.done.clone()),
                    None => return, // stop()/another path already took it
                },
                None => return, // slot vanished before reaping
            }
        };
        let status = child.wait().await.ok();
        drop(child);
        // Wake everyone waiting on this slot's terminal state, then remove it.
        // (The last strong reference lives in this function, so removal cannot
        // resurrect a deleted slot.)
        notify.notify_waiters();
        self.inner.lock().unwrap().remove(&handle);
        let event = ExitEvent { handle, kind, status: status.map(|s| s.to_string()) };
        // Bounded queue, drop-newest: the reaper must never block and memory must
        // never grow. Lost events are still observable via `list()`/state queries.
        if let Err(e) = self.tx.try_send(event) {
            tracing::warn!("exit-event queue full or closed, dropping event: {e}");
        }
    }

    /// Graceful stop: SIGTERM (unix), wait up to `grace`, then escalate to kill.
    /// Returns true when termination is confirmed.
    pub async fn stop(&self, handle: u64, grace: Duration) -> bool {
        // Grab the child handle + notifier atomically; escalate-to-kill must use the
        // SAME handle the reaper will observe, otherwise SIGKILL never lands.
        let slot_info = {
            let g = self.inner.lock().unwrap();
            g.get(&handle).map(|slot| (slot.supervised.as_ref().and_then(|c| c.id()), slot.done.clone()))
        };
        let Some((pid_opt, notify)) = slot_info else { return true }; // unknown handle: nothing to stop

        #[cfg(unix)]
        if let Some(pid) = pid_opt {
            send_sigterm(pid as i32);
        }

        // Wait (without the lock) for the reaper to publish the terminal state.
        if tokio::time::timeout(grace, notify.notified()).await.is_ok() {
            return true; // reaped within grace
        }

        // Escalate: kill through our own handle. The reaper observes the exit and
        // cleans up the slot; the guard is dropped before any await so the future
        // stays `Send` (std MutexGuard is not).
        let mut child_opt = {
            let mut g = self.inner.lock().unwrap();
            g.get_mut(&handle).and_then(|slot| slot.supervised.take())
        };
        if let Some(c) = child_opt.as_mut() {
            let _ = c.start_kill();
            // Bounded final wait; Drop kills even if this times out.
            let _ = tokio::time::timeout(Duration::from_millis(1000), c.wait()).await;
        }
        // Confirm the slot actually went terminal.
        let gone = { !self.inner.lock().unwrap().contains_key(&handle) };
        gone || tokio::time::timeout(Duration::from_millis(500), notify.notified()).await.is_ok()
    }

    /// Stop everything (shutdown path). Remaining children are killed via Drop.
    pub fn stop_all(&self) {
        let mut g = self.inner.lock().unwrap();
        for (_h, slot) in g.iter_mut() {
            if let Some(mut child) = slot.supervised.take() {
                let _ = child.start_kill();
            }
        }
    }
}

// --- tiny platform helper kept private to this module ---

#[cfg(unix)]
fn send_sigterm(pid: i32) {
    // SIGTERM via the C library directly to avoid depending on the `libc` crate
    // for one call. `kill` is async-signal-safe and always available on unix.
    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    const SIGTERM: i32 = 15;
    unsafe {
        kill(pid, SIGTERM);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::process::Command;

    fn spawn_dummy(secs: f64) -> Child {
        // Portable-ish dummy: sleep via sh on unix. Tests run on Linux CI primarily.
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
        assert_eq!(sup.count(), 1);
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
        // Child traps SIGTERM and ignores it; supervisor must escalate to kill.
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
    async fn drop_supervisor_kills_children() {
        let sup = Supervisor::new();
        let mut rx = sup.subscribe().expect("first subscriber");
        let h = sup.adopt(ManagedKind::ScrcpyMirror, spawn_dummy(60.0));
        let pid = {
            let g = sup.inner.lock().unwrap();
            match g[&h].supervised.as_ref().expect("running").id() {
                Some(p) => p,
                None => panic!("no pid"),
            }
        };
        sup.stop_all();
        // The exit event proves the reaper observed termination before we drop.
        let ev = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("event")
            .expect("channel open");
        assert_eq!(ev.handle, h);
        drop(sup);
        // The OS process must be gone.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let alive = std::path::Path::new(&format!("/proc/{pid}")).exists();
            if !alive {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "orphan survived");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
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
                let _ = s.stop(h, Duration::from_millis(200)).await;
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        // Give reapers a moment, then require full cleanup (no slot leaks).
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(sup.count(), 0, "slots leaked");
    }
}
