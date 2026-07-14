use std::{collections::HashMap, sync::Arc};

use tokio::sync::broadcast;

/// Lines buffered per subscriber before a slow client starts missing them.
const CHANNEL_CAPACITY: usize = 256;

/// Fans out tailed lines without opening another upstream connection for each
/// HTTP subscriber.
#[derive(Debug)]
pub struct LiveBroadcaster {
    senders: HashMap<String, broadcast::Sender<Arc<str>>>,
}

impl LiveBroadcaster {
    pub fn new(stream_names: impl IntoIterator<Item = String>) -> Self {
        let senders = stream_names
            .into_iter()
            .map(|name| (name, broadcast::channel(CHANNEL_CAPACITY).0))
            .collect();
        Self { senders }
    }

    pub fn publish(&self, stream_name: &str, line: &str) {
        let Some(sender) = self.senders.get(stream_name) else {
            return;
        };
        if sender.receiver_count() == 0 {
            return;
        }
        // `send` can still fail if all subscribers disconnect after the count.
        let _ = sender.send(Arc::from(line));
    }

    pub fn subscribe(&self, stream_name: &str) -> Option<broadcast::Receiver<Arc<str>>> {
        self.senders
            .get(stream_name)
            .map(broadcast::Sender::subscribe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subscriber_receives_published_lines() {
        let broadcaster = LiveBroadcaster::new(["nhk".to_owned()]);
        let mut receiver = broadcaster.subscribe("nhk").unwrap();

        broadcaster.publish("nhk", "line-1");

        assert_eq!(receiver.recv().await.unwrap().as_ref(), "line-1");
    }

    #[test]
    fn unknown_stream_has_no_sender() {
        let broadcaster = LiveBroadcaster::new(["nhk".to_owned()]);

        assert!(broadcaster.subscribe("other").is_none());
    }

    #[test]
    fn publishing_without_subscribers_succeeds() {
        let broadcaster = LiveBroadcaster::new(["nhk".to_owned()]);

        broadcaster.publish("nhk", "line-1");
    }
}
