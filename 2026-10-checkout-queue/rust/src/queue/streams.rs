use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::watch;

use super::TicketView;

type Open = HashMap<String, Vec<(u64, watch::Sender<Option<TicketView>>)>>;

/// The server-sent event streams open on this instance, by ticket. A stream only ever needs the
/// latest view, so an unsent older one is simply replaced (a `watch` channel holds one value).
#[derive(Debug, Default)]
pub struct TicketStreams {
    open: Mutex<Open>,
    next_id: AtomicU64,
}

impl TicketStreams {
    pub fn open(self: &Arc<Self>, ticket_id: &str) -> TicketStream {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, updates) = watch::channel(None);
        self.lock()
            .entry(ticket_id.to_owned())
            .or_default()
            .push((id, sender));
        TicketStream {
            streams: Arc::clone(self),
            ticket_id: ticket_id.to_owned(),
            id,
            updates,
        }
    }

    /// Tickets with at least one open stream here.
    pub fn ticket_ids(&self) -> Vec<String> {
        self.lock().keys().cloned().collect()
    }

    /// Hands the view to every stream open for its ticket, replacing any view not yet sent.
    pub fn publish(&self, view: &TicketView) {
        if let Some(streams) = self.lock().get(&view.ticket_id) {
            for (_, sender) in streams {
                sender.send_replace(Some(view.clone()));
            }
        }
    }

    fn close(&self, ticket_id: &str, id: u64) {
        let mut open = self.lock();
        if let Some(streams) = open.get_mut(ticket_id) {
            streams.retain(|(stream_id, _)| *stream_id != id);
            if streams.is_empty() {
                open.remove(ticket_id);
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, Open> {
        // Nothing here can be left half-updated by a panic, so a poisoned lock is still usable.
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One open stream. Dropping it (the client went away) unregisters it.
#[derive(Debug)]
pub struct TicketStream {
    streams: Arc<TicketStreams>,
    ticket_id: String,
    id: u64,
    updates: watch::Receiver<Option<TicketView>>,
}

impl TicketStream {
    /// Waits for the next pushed view. `None` only if the registry let go of this stream.
    pub async fn next(&mut self) -> Option<TicketView> {
        loop {
            self.updates.changed().await.ok()?;
            if let Some(view) = self.updates.borrow_and_update().clone() {
                return Some(view);
            }
        }
    }
}

impl Drop for TicketStream {
    fn drop(&mut self) {
        self.streams.close(&self.ticket_id, self.id);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::super::TicketStatus;
    use super::*;

    fn view(ticket_id: &str, position: i64) -> TicketView {
        TicketView {
            ticket_id: ticket_id.to_owned(),
            user_id: "u".to_owned(),
            seq: 1,
            status: TicketStatus::Waiting,
            position: Some(position),
            estimated_wait_seconds: None,
            admitted_at_ms: None,
            admission_expires_at_ms: None,
            order_id: None,
            poll_after_ms: 1000,
        }
    }

    #[tokio::test]
    async fn publish_reaches_every_stream_of_that_ticket_only() {
        let streams = Arc::new(TicketStreams::default());
        let mut first = streams.open("a");
        let mut second = streams.open("a");
        let mut other = streams.open("b");

        streams.publish(&view("a", 3));

        assert_eq!(first.next().await.unwrap().position, Some(3));
        assert_eq!(second.next().await.unwrap().position, Some(3));
        let nothing = tokio::time::timeout(Duration::from_millis(20), other.next()).await;
        assert!(nothing.is_err(), "b got a view meant for a");
    }

    #[tokio::test]
    async fn a_slow_stream_only_sees_the_latest_view() {
        let streams = Arc::new(TicketStreams::default());
        let mut stream = streams.open("a");

        streams.publish(&view("a", 3));
        streams.publish(&view("a", 2));

        assert_eq!(stream.next().await.unwrap().position, Some(2));
    }

    #[test]
    fn dropping_a_stream_unregisters_it() {
        let streams = Arc::new(TicketStreams::default());
        let first = streams.open("a");
        let second = streams.open("a");
        let other = streams.open("b");

        drop(first);
        let mut open = streams.ticket_ids();
        open.sort();
        assert_eq!(open, ["a", "b"]);

        drop(second);
        drop(other);
        assert!(streams.ticket_ids().is_empty());
        streams.publish(&view("a", 1));
    }
}
