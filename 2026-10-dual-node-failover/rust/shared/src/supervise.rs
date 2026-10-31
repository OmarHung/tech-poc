//! Background loops that must outlive their own bugs.

use std::any::Any;
use std::future::Future;
use std::time::Duration;

use tokio::task::{JoinHandle, JoinSet};

/// How long a crashed loop waits before it is started again.
pub const RESTART_DELAY: Duration = Duration::from_secs(1);

/// Runs `make()` as a task and starts it again if it ever returns or panics, logging why each time.
///
/// A panic inside a tokio task only ends that task: without this the HTTP side of a node would keep
/// answering while, say, its election loop had silently stopped. Aborting the returned handle also
/// aborts the running loop.
pub fn spawn_supervised<F, Fut>(name: &'static str, make: F) -> JoinHandle<()>
where
    F: Fn() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            // A JoinSet aborts what it holds when dropped, i.e. when this supervisor is aborted.
            let mut running = JoinSet::new();
            running.spawn(make());
            match running.join_next().await {
                Some(Ok(())) => tracing::error!("{name} stopped unexpectedly; restarting it"),
                Some(Err(error)) if error.is_panic() => tracing::error!(
                    "{name} crashed: {}; restarting it",
                    panic_message(error.into_panic().as_ref())
                ),
                Some(Err(_)) | None => return,
            }
            tokio::time::sleep(RESTART_DELAY).await;
        }
    })
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn restarts_a_loop_that_panics_or_returns() {
        let starts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&starts);
        let handle = spawn_supervised("test loop", move || {
            let counter = Arc::clone(&counter);
            async move {
                match counter.fetch_add(1, Ordering::SeqCst) {
                    0 => panic!("boom"),
                    1 => {}
                    _ => std::future::pending::<()>().await,
                }
            }
        });

        tokio::time::sleep(RESTART_DELAY * 3).await;
        assert_eq!(starts.load(Ordering::SeqCst), 3);
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
    }

    #[test]
    fn reads_panic_payloads() {
        assert_eq!(panic_message(&"static"), "static");
        assert_eq!(panic_message(&String::from("owned")), "owned");
        assert_eq!(panic_message(&42), "unknown panic");
    }
}
