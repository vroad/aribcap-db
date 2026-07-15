use std::{collections::HashSet, future::Future, time::Duration};

use anyhow::{Context, Result, bail};
use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    config::{Config, ResolvedStream},
    stream,
};

/// Delay between stream reconnection attempts.
const RECONNECT_DELAY: Duration = Duration::from_secs(15);

/// Upper bound on waiting for aborted tasks during shutdown.
const ABORT_JOIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Maximum number of lines buffered for each consumer.
const LINE_CHANNEL_CAPACITY: usize = 256;

#[derive(Debug)]
pub struct StreamLine {
    pub stream_name: String,
    pub label: String,
    pub line: String,
}

pub fn resolve_targets(
    config: &Config,
    streams: &[String],
    all: bool,
) -> Result<Vec<ResolvedStream>> {
    match (all, streams.is_empty()) {
        (true, true) => config.resolve_all_streams(),
        (true, false) => bail!("use either --stream <NAME> or --all, not both"),
        (false, true) => bail!("use --stream <NAME> or --all"),
        (false, false) => {
            let mut seen = HashSet::new();
            if let Some(duplicate) = streams.iter().find(|stream| !seen.insert(stream.as_str())) {
                bail!("stream '{duplicate}' was specified more than once");
            }
            config.resolve_streams(streams)
        }
    }
}

pub async fn tail_targets<OnLine, OnLineFuture, ShutdownFuture>(
    client: reqwest::Client,
    targets: Vec<ResolvedStream>,
    on_line: OnLine,
    shutdown: ShutdownFuture,
) -> Result<()>
where
    OnLine: Fn(StreamLine) -> OnLineFuture + Clone + Send + 'static,
    OnLineFuture: Future<Output = Result<()>> + Send + 'static,
    ShutdownFuture: Future<Output = Result<()>> + Send,
{
    let mut tasks = FuturesUnordered::new();

    for target in targets {
        let (tx, rx) = mpsc::channel::<StreamLine>(LINE_CHANNEL_CAPACITY);
        let producer = tokio::spawn(tail_target(client.clone(), target, tx));
        let consumer = tokio::spawn(consume_lines(rx, on_line.clone()));
        tasks.push(producer);
        tasks.push(consumer);
    }

    drive_tasks(&mut tasks, shutdown).await
}

/// Tails each target under an independent supervisor. If a target's line
/// handler fails, its supervisor waits for the normal reconnect delay and
/// restarts only that target.
pub async fn tail_targets_isolated<
    OnConnect,
    OnConnectFuture,
    OnLine,
    OnLineFuture,
    ShutdownFuture,
>(
    client: reqwest::Client,
    targets: Vec<ResolvedStream>,
    on_connect: OnConnect,
    on_line: OnLine,
    shutdown: ShutdownFuture,
) -> Result<()>
where
    OnConnect: Fn(String) -> OnConnectFuture + Clone + Send + 'static,
    OnConnectFuture: Future<Output = Result<()>> + Send + 'static,
    OnLine: Fn(StreamLine) -> OnLineFuture + Clone + Send + 'static,
    OnLineFuture: Future<Output = Result<()>> + Send + 'static,
    ShutdownFuture: Future<Output = Result<()>> + Send,
{
    let mut tasks = FuturesUnordered::new();

    for target in targets {
        tasks.push(tokio::spawn(supervise_target(
            client.clone(),
            target,
            on_connect.clone(),
            on_line.clone(),
        )));
    }

    drive_tasks(&mut tasks, shutdown).await
}

/// Drives tasks until shutdown, a task failure, or all tasks complete. On shutdown or failure,
/// aborts remaining tasks and waits up to the timeout for them to finish.
async fn drive_tasks<ShutdownFuture>(
    tasks: &mut FuturesUnordered<JoinHandle<Result<()>>>,
    shutdown: ShutdownFuture,
) -> Result<()>
where
    ShutdownFuture: Future<Output = Result<()>> + Send,
{
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            signal = &mut shutdown => {
                let result = signal;
                abort_and_join(tasks, ABORT_JOIN_TIMEOUT).await;
                result?;
                return Ok(());
            }
            result = tasks.next(), if !tasks.is_empty() => {
                match result {
                    Some(result) => {
                        if let Err(error) = flatten_task_result(result) {
                            abort_and_join(tasks, ABORT_JOIN_TIMEOUT).await;
                            return Err(error);
                        }
                    }
                    None => return Ok(()),
                }
            }
        }
    }
}

fn flatten_task_result(
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    result.context("tail task failed")?
}

/// Processes one stream's lines in order, one call to `on_line` at a time.
/// Separate consumer tasks let different streams run concurrently.
async fn consume_lines<OnLine, OnLineFuture>(
    mut rx: mpsc::Receiver<StreamLine>,
    on_line: OnLine,
) -> Result<()>
where
    OnLine: Fn(StreamLine) -> OnLineFuture + Send + 'static,
    OnLineFuture: Future<Output = Result<()>> + Send + 'static,
{
    while let Some(event) = rx.recv().await {
        on_line(event).await?;
    }
    Ok(())
}

async fn supervise_target<OnConnect, OnConnectFuture, OnLine, OnLineFuture>(
    client: reqwest::Client,
    target: ResolvedStream,
    on_connect: OnConnect,
    on_line: OnLine,
) -> Result<()>
where
    OnConnect: Fn(String) -> OnConnectFuture + Clone + Send + 'static,
    OnConnectFuture: Future<Output = Result<()>> + Send + 'static,
    OnLine: Fn(StreamLine) -> OnLineFuture + Clone + Send + 'static,
    OnLineFuture: Future<Output = Result<()>> + Send + 'static,
{
    loop {
        let result = run_target_attempt(
            client.clone(),
            target.clone(),
            on_connect.clone(),
            on_line.clone(),
        )
        .await;

        match result {
            Ok(()) => tracing::error!(
                stream = target.name,
                retry_in = ?RECONNECT_DELAY,
                "Stream processing stopped unexpectedly",
            ),
            Err(error) => tracing::error!(
                stream = target.name,
                error = ?error,
                retry_in = ?RECONNECT_DELAY,
                "Stream processing failed",
            ),
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

async fn run_target_attempt<OnConnect, OnConnectFuture, OnLine, OnLineFuture>(
    client: reqwest::Client,
    target: ResolvedStream,
    on_connect: OnConnect,
    on_line: OnLine,
) -> Result<()>
where
    OnConnect: Fn(String) -> OnConnectFuture + Clone + Send + 'static,
    OnConnectFuture: Future<Output = Result<()>> + Send + 'static,
    OnLine: Fn(StreamLine) -> OnLineFuture + Send + 'static,
    OnLineFuture: Future<Output = Result<()>> + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<StreamLine>(LINE_CHANNEL_CAPACITY);
    let producer = tail_target_with_connect(client, target, tx, on_connect);
    let consumer = consume_lines(rx, on_line);
    tokio::pin!(producer, consumer);

    tokio::select! {
        result = &mut producer => result.context("stream producer stopped"),
        result = &mut consumer => result.context("stream consumer stopped"),
    }
}

async fn tail_target(
    client: reqwest::Client,
    target: ResolvedStream,
    tx: mpsc::Sender<StreamLine>,
) -> Result<()> {
    tail_target_with_connect(client, target, tx, |_| async { Ok(()) }).await
}

async fn tail_target_with_connect<OnConnect, OnConnectFuture>(
    client: reqwest::Client,
    target: ResolvedStream,
    tx: mpsc::Sender<StreamLine>,
    on_connect: OnConnect,
) -> Result<()>
where
    OnConnect: Fn(String) -> OnConnectFuture + Clone,
    OnConnectFuture: Future<Output = Result<()>>,
{
    let stream_name = target.name;
    let label = target.label;
    let url = target.url;
    let connect = || {
        let client = client.clone();
        let url = url.clone();
        let tx = tx.clone();
        let stream_name = stream_name.clone();
        let label = label.clone();
        async move {
            tracing::info!(stream = stream_name, label, url, "Connecting to stream");
            stream::tail_once(&client, &url, |line| {
                let tx = tx.clone();
                let stream_name = stream_name.clone();
                let label = label.clone();
                async move {
                    tx.send(StreamLine {
                        stream_name,
                        label,
                        line,
                    })
                    .await
                    .map_err(|_| anyhow::anyhow!("output channel closed"))
                }
            })
            .await
        }
    };

    let connect_stream_name = stream_name.clone();
    let before_connect = move || {
        let on_connect = on_connect.clone();
        let stream_name = connect_stream_name.clone();
        async move { on_connect(stream_name).await }
    };

    pump_stream(&stream_name, before_connect, connect, &tx, || {
        tokio::time::sleep(RECONNECT_DELAY)
    })
    .await
}

/// Reconnects after stream failures until the consumer closes.
/// Retries use a fixed delay without a cap or backoff.
/// Injected connection and sleep functions keep retry behavior testable.
async fn pump_stream<
    BeforeConnect,
    BeforeConnectFuture,
    Connect,
    ConnectFuture,
    Sleep,
    SleepFuture,
>(
    stream_name: &str,
    mut before_connect: BeforeConnect,
    mut connect: Connect,
    tx: &mpsc::Sender<StreamLine>,
    mut sleep: Sleep,
) -> Result<()>
where
    BeforeConnect: FnMut() -> BeforeConnectFuture,
    BeforeConnectFuture: Future<Output = Result<()>>,
    Connect: FnMut() -> ConnectFuture,
    ConnectFuture: Future<Output = Result<()>>,
    Sleep: FnMut() -> SleepFuture,
    SleepFuture: Future<Output = ()>,
{
    loop {
        // Consumer gone before we (re)connect: shut down quietly.
        if tx.is_closed() {
            return Ok(());
        }
        before_connect().await?;
        match connect().await {
            // The consumer may close while `connect` is running, causing a send to `tx`
            // to fail. Re-check `tx.is_closed()` before deciding to reconnect.
            Err(_) if tx.is_closed() => return Ok(()),
            Err(error) => {
                tracing::warn!(
                    stream = stream_name,
                    %error,
                    "Stream disconnected; reconnecting in {}s",
                    RECONNECT_DELAY.as_secs(),
                );
                tokio::select! {
                    _ = tx.closed() => return Ok(()),
                    _ = sleep() => {}
                }
            }
            Ok(()) => unreachable!("connect only returns via error"),
        }
    }
}

/// Aborts every producer and consumer task and waits up to `timeout` for them
/// to finish. After `timeout`, shutdown proceeds without the remaining tasks
/// instead of waiting for them forever.
async fn abort_and_join(tasks: &mut FuturesUnordered<JoinHandle<Result<()>>>, timeout: Duration) {
    for task in tasks.iter() {
        task.abort();
    }
    let join_all = async { while tasks.next().await.is_some() {} };
    if tokio::time::timeout(timeout, join_all).await.is_err() {
        tracing::warn!("aborted tasks did not stop in time; skipping the clean join");
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use tokio::sync::Notify;

    use super::*;

    fn line(label: &str, text: &str) -> StreamLine {
        StreamLine {
            stream_name: label.to_owned(),
            label: label.to_owned(),
            line: text.to_owned(),
        }
    }

    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    async fn spawn_tracked_pending_task() -> (JoinHandle<Result<()>>, Arc<AtomicBool>) {
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        let started = Arc::new(Notify::new());
        let task_started = started.clone();
        let task = tokio::spawn(async move {
            let _guard = Dropped(task_dropped);
            task_started.notify_one();
            std::future::pending::<()>().await;
            Ok(())
        });
        started.notified().await;
        (task, dropped)
    }

    async fn wait_for(notification: &Notify) {
        tokio::time::timeout(Duration::from_secs(1), notification.notified())
            .await
            .unwrap();
    }

    #[test]
    fn resolve_targets_rejects_duplicate_streams() {
        let config: Config = toml::from_str(
            r#"
[upstream]
url_template = "http://example.test/{{ channel }}"

[streams.nhk]
vars.channel = "nhk"
"#,
        )
        .unwrap();
        let streams = vec!["nhk".to_owned(), "nhk".to_owned()];

        let error = resolve_targets(&config, &streams, false).unwrap_err();

        assert_eq!(
            error.to_string(),
            "stream 'nhk' was specified more than once"
        );
    }

    #[tokio::test]
    async fn shutdown_aborts_and_joins_tasks() {
        let (task, dropped) = spawn_tracked_pending_task().await;
        let mut tasks = FuturesUnordered::new();
        tasks.push(task);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        shutdown_tx.send(()).unwrap();

        drive_tasks(&mut tasks, async move {
            shutdown_rx.await.context("shutdown sender dropped")?;
            Ok(())
        })
        .await
        .unwrap();

        assert!(dropped.load(Ordering::SeqCst));
        assert!(tasks.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abort_and_join_returns_when_a_task_cannot_be_cancelled() {
        let finished = Arc::new(AtomicBool::new(false));
        let task_finished = finished.clone();
        let started = Arc::new(Notify::new());
        let task_started = started.clone();
        let mut tasks = FuturesUnordered::new();
        tasks.push(tokio::spawn(async move {
            task_started.notify_one();
            // A blocking call with no await point: abort cannot take effect.
            std::thread::sleep(Duration::from_millis(500));
            task_finished.store(true, Ordering::SeqCst);
            Ok(())
        }));
        started.notified().await;

        abort_and_join(&mut tasks, Duration::from_millis(50)).await;

        // The task sets `finished` when it completes. `finished` still being
        // false here proves that abort_and_join returned at the timeout
        // instead of waiting for the task.
        assert!(!finished.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn task_error_aborts_and_joins_remaining_tasks() {
        let (task, dropped) = spawn_tracked_pending_task().await;
        let mut tasks = FuturesUnordered::new();
        tasks.push(task);
        tasks.push(tokio::spawn(async {
            Err(anyhow::anyhow!("consumer failed"))
        }));

        let result = drive_tasks(&mut tasks, std::future::pending()).await;

        assert_eq!(result.unwrap_err().to_string(), "consumer failed");
        assert!(dropped.load(Ordering::SeqCst));
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn isolated_target_error_does_not_stop_other_targets() {
        let a_started = Arc::new(Notify::new());
        let b_started = Arc::new(Notify::new());
        let before_connect = {
            let a_started = a_started.clone();
            let b_started = b_started.clone();
            move |stream_name: String| {
                let a_started = a_started.clone();
                let b_started = b_started.clone();
                async move {
                    if stream_name == "a" {
                        a_started.notify_one();
                        Err(anyhow::anyhow!("archive failed"))
                    } else {
                        b_started.notify_one();
                        Ok(())
                    }
                }
            }
        };
        let targets = ["a", "b"]
            .into_iter()
            .map(|name| ResolvedStream {
                name: name.to_owned(),
                label: name.to_owned(),
                url: "http://127.0.0.1:1".to_owned(),
            })
            .collect();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(tail_targets_isolated(
            reqwest::Client::new(),
            targets,
            before_connect,
            |_| async { Ok(()) },
            async move {
                shutdown_rx.await.context("shutdown sender dropped")?;
                Ok(())
            },
        ));

        wait_for(&a_started).await;
        wait_for(&b_started).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());

        shutdown_tx.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn pump_stream_retries_until_consumer_closes() {
        let (tx, rx) = mpsc::channel::<StreamLine>(LINE_CHANNEL_CAPACITY);
        let rx = Cell::new(Some(rx));
        let attempts = Cell::new(0usize);
        let before_connect_calls = Cell::new(0usize);
        let sleep_calls = Cell::new(0usize);

        let before_connect = || {
            before_connect_calls.set(before_connect_calls.get() + 1);
            std::future::ready(Ok(()))
        };
        let connect = || {
            attempts.set(attempts.get() + 1);
            std::future::ready(Err(anyhow::anyhow!("connect failed")))
        };
        let sleep = || {
            let sleep_count = sleep_calls.get() + 1;
            sleep_calls.set(sleep_count);
            // Close the consumer after two failures; the next iteration must stop.
            if sleep_count == 2 {
                rx.take();
            }
            std::future::ready(())
        };

        pump_stream("test", before_connect, connect, &tx, sleep)
            .await
            .unwrap();
        assert_eq!(attempts.get(), 2);
        assert_eq!(before_connect_calls.get(), 2);
        assert_eq!(sleep_calls.get(), 2);
    }

    #[tokio::test]
    async fn pump_stream_skips_connect_when_consumer_is_closed() {
        let (tx, rx) = mpsc::channel::<StreamLine>(LINE_CHANNEL_CAPACITY);
        drop(rx);
        let attempts = Cell::new(0usize);
        let before_connect_calls = Cell::new(0usize);
        let sleep_calls = Cell::new(0usize);

        let before_connect = || {
            before_connect_calls.set(before_connect_calls.get() + 1);
            std::future::ready(Ok(()))
        };
        let connect = || {
            attempts.set(attempts.get() + 1);
            std::future::ready(Err(anyhow::anyhow!("connect failed")))
        };
        let sleep = || {
            sleep_calls.set(sleep_calls.get() + 1);
            std::future::ready(())
        };

        pump_stream("test", before_connect, connect, &tx, sleep)
            .await
            .unwrap();
        assert_eq!(attempts.get(), 0);
        assert_eq!(before_connect_calls.get(), 0);
        assert_eq!(sleep_calls.get(), 0);
    }

    #[tokio::test]
    async fn pump_stream_stops_when_before_connect_fails() {
        let (tx, _rx) = mpsc::channel::<StreamLine>(LINE_CHANNEL_CAPACITY);
        let attempts = Cell::new(0usize);
        let sleep_calls = Cell::new(0usize);
        let before_connect = || std::future::ready(Err(anyhow::anyhow!("reset failed")));
        let connect = || {
            attempts.set(attempts.get() + 1);
            std::future::ready(Err(anyhow::anyhow!("connect failed")))
        };
        let sleep = || {
            sleep_calls.set(sleep_calls.get() + 1);
            std::future::ready(())
        };

        let error = pump_stream("test", before_connect, connect, &tx, sleep)
            .await
            .unwrap_err();

        assert_eq!(error.to_string(), "reset failed");
        assert_eq!(attempts.get(), 0);
        assert_eq!(sleep_calls.get(), 0);
    }

    #[tokio::test]
    async fn blocked_stream_does_not_block_others() {
        let first_a_should_block = Arc::new(AtomicBool::new(true));
        let a_parked = Arc::new(Notify::new());
        let release_a = Arc::new(Notify::new());
        let b_done = Arc::new(Notify::new());
        let a_lines = Arc::new(Mutex::new(Vec::new()));
        let b_count = Arc::new(AtomicUsize::new(0));

        let on_a = {
            let first_a_should_block = first_a_should_block.clone();
            let a_parked = a_parked.clone();
            let release_a = release_a.clone();
            let a_lines = a_lines.clone();
            move |event: StreamLine| {
                let first_a_should_block = first_a_should_block.clone();
                let a_parked = a_parked.clone();
                let release_a = release_a.clone();
                let a_lines = a_lines.clone();
                async move {
                    if first_a_should_block.swap(false, Ordering::SeqCst) {
                        a_parked.notify_one();
                        release_a.notified().await;
                    }
                    a_lines.lock().unwrap().push(event.line);
                    Ok(())
                }
            }
        };
        let on_b = {
            let b_done = b_done.clone();
            let b_count = b_count.clone();
            move |_| {
                let b_done = b_done.clone();
                let b_count = b_count.clone();
                async move {
                    if b_count.fetch_add(1, Ordering::SeqCst) + 1 == 3 {
                        b_done.notify_one();
                    }
                    Ok(())
                }
            }
        };

        let (tx_a, rx_a) = mpsc::channel::<StreamLine>(LINE_CHANNEL_CAPACITY);
        let (tx_b, rx_b) = mpsc::channel::<StreamLine>(LINE_CHANNEL_CAPACITY);
        let consumer_a = tokio::spawn(consume_lines(rx_a, on_a));
        let consumer_b = tokio::spawn(consume_lines(rx_b, on_b));

        // Send the first line for stream "a" and wait until its consumer pauses
        // while processing it.
        tx_a.send(line("a", "a1")).await.unwrap();
        wait_for(&a_parked).await;

        // Send three lines for stream "b" and verify that its consumer processes
        // all of them while stream "a" remains paused.
        for i in 0..3 {
            tx_b.send(line("b", &format!("b{i}"))).await.unwrap();
        }
        wait_for(&b_done).await;
        assert_eq!(b_count.load(Ordering::SeqCst), 3);
        assert!(a_lines.lock().unwrap().is_empty());

        // Resume stream "a" and verify that its consumer processes both lines in order.
        tx_a.send(line("a", "a2")).await.unwrap();
        release_a.notify_one();

        drop(tx_a);
        drop(tx_b);
        consumer_a.await.unwrap().unwrap();
        consumer_b.await.unwrap().unwrap();
        assert_eq!(*a_lines.lock().unwrap(), vec!["a1", "a2"]);
    }

    #[tokio::test]
    async fn consumer_processes_lines_in_order() {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let on_line = {
            let lines = lines.clone();
            move |event: StreamLine| {
                let lines = lines.clone();
                async move {
                    lines.lock().unwrap().push(event.line);
                    Ok(())
                }
            }
        };
        let (tx, rx) = mpsc::channel::<StreamLine>(LINE_CHANNEL_CAPACITY);
        let consumer = tokio::spawn(consume_lines(rx, on_line));

        tx.send(line("a", "a1")).await.unwrap();
        tx.send(line("a", "a2")).await.unwrap();
        drop(tx);
        consumer.await.unwrap().unwrap();

        assert_eq!(*lines.lock().unwrap(), vec!["a1", "a2"]);
    }
}
