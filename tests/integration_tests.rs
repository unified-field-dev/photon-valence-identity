//! Integration tests for Photon publish/subscribe and executor wiring.
//!
//! Test-only topic/handler fixtures below are intentionally undocumented; this binary target is
//! exempt from the library's `missing_docs = "deny"` lint (see `Cargo.toml`).
#![allow(missing_docs)]

mod common;

use futures::StreamExt;
use photon::{configure, Photon, SubscribeOpts};
use serde_json::json;
use serial_test::serial;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::time::{timeout, Duration};

#[photon::topic(name = "test.executor.events")]
pub struct TestExecutorEvent {
    pub value: u32,
}

static HANDLER_INVOCATION_COUNT: AtomicU32 = AtomicU32::new(0);

#[photon::subscribe(topic = "test.executor.events", durable = "test.executor.events.sub")]
#[allow(clippy::unused_async)]
pub async fn on_test_executor_event(
    actor: Box<dyn photon::Actor>,
    ev: TestExecutorEvent,
) -> photon::Result<()> {
    let _ = actor;
    HANDLER_INVOCATION_COUNT.fetch_add(1, Ordering::SeqCst);
    let _ = ev;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_publish_returns_event_id() {
    common::ensure_photon_test_env();
    let photon = Photon::builder().build().expect("build");

    let topic_name = "test.events";
    let payload = json!({"message": "hello"});
    let actor_json = json!({"System": {"operation": "test"}});

    let event_id = photon
        .publish(topic_name, None, actor_json.clone(), payload)
        .await
        .expect("publish");

    assert_ne!(event_id, "");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_then_publish_receives_event() {
    common::ensure_photon_test_env();
    let photon = Arc::new(Photon::builder().build().expect("build"));

    let topic_name = "test.sub.pub";
    let actor_json = json!({"System": {"operation": "test"}});

    let mut stream = photon.subscribe(topic_name, None, None);

    let photon_clone = Arc::clone(&photon);
    let actor_clone = actor_json.clone();
    let event_id = tokio::spawn(async move {
        photon_clone
            .publish(topic_name, None, actor_clone, json!({"n": 1}))
            .await
            .expect("publish")
    })
    .await
    .expect("spawn");

    let received = timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timeout")
        .expect("stream item");

    let evt = received.expect("event ok");
    assert_eq!(evt.event_id, event_id);
    assert_eq!(evt.topic_name, topic_name);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_subscribers_receive_same_event() {
    common::ensure_photon_test_env();
    let photon = Arc::new(Photon::builder().build().expect("build"));

    let topic_name = "test.multi";
    let actor_json = json!({"System": {"operation": "test"}});

    let mut s1 = photon.subscribe(topic_name, None, None);
    let mut s2 = photon.subscribe(topic_name, None, None);

    let event_id = photon
        .publish(topic_name, None, actor_json.clone(), json!({"n": 1}))
        .await
        .expect("publish");

    let r1 = timeout(Duration::from_secs(2), s1.next())
        .await
        .expect("t1")
        .expect("s1")
        .expect("ok");
    let r2 = timeout(Duration::from_secs(2), s2.next())
        .await
        .expect("t2")
        .expect("s2")
        .expect("ok");

    assert_eq!(r1.event_id, event_id);
    assert_eq!(r2.event_id, event_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_keyed_filtering_only_receives_matching_key() {
    common::ensure_photon_test_env();
    let photon = Arc::new(Photon::builder().build().expect("build"));

    let topic_name = "test.keyed";
    let actor_json = json!({"System": {"operation": "test"}});

    let mut stream_a = photon.subscribe(topic_name, Some("key-a"), None);

    photon
        .publish(
            topic_name,
            Some("key-b"),
            actor_json.clone(),
            json!({"key": "b"}),
        )
        .await
        .expect("publish key-b");

    let event_id_a = photon
        .publish(
            topic_name,
            Some("key-a"),
            actor_json.clone(),
            json!({"key": "a"}),
        )
        .await
        .expect("publish key-a");

    let received = timeout(Duration::from_secs(2), stream_a.next())
        .await
        .expect("timeout")
        .expect("stream item")
        .expect("ok");

    assert_eq!(received.event_id, event_id_a);
    assert_eq!(received.topic_key, Some("key-a".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_executor_invokes_handler() {
    HANDLER_INVOCATION_COUNT.store(0, Ordering::SeqCst);

    common::ensure_photon_test_env();
    let photon = Arc::new(Photon::builder().auto_registry().build().expect("build"));
    configure((*photon).clone());

    common::start_photon_executor(photon.as_ref(), common::TestValenceFactory::arc())
        .expect("start executor");

    tokio::time::sleep(Duration::from_millis(200)).await;

    TestExecutorEvent { value: 42 }
        .publish()
        .await
        .expect("publish");

    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if HANDLER_INVOCATION_COUNT.load(Ordering::SeqCst) >= 1 {
            break;
        }
    }
    assert!(
        HANDLER_INVOCATION_COUNT.load(Ordering::SeqCst) >= 1,
        "handler should have been invoked"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_typed_subscribe_returns_envelope_stream() {
    common::ensure_photon_test_env();
    let photon = Arc::new(Photon::builder().auto_registry().build().expect("build"));
    configure((*photon).clone());

    let opts = SubscribeOpts::default_ephemeral();
    let mut stream = TestExecutorEvent::subscribe(opts).await.expect("subscribe");

    TestExecutorEvent { value: 99 }
        .publish()
        .await
        .expect("publish");

    let received = timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timeout")
        .expect("stream item")
        .expect("envelope ok");
    assert_eq!(received.payload.value, 99);
    assert_eq!(received.event.topic_name, "test.executor.events");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_after_seq_replays_from_buffer() {
    common::ensure_photon_test_env();
    let photon = Arc::new(Photon::builder().build().expect("build"));

    let topic_name = "test.replay.seq";
    let actor_json = json!({"System": {"operation": "test"}});

    photon
        .publish(topic_name, None, actor_json.clone(), json!({"value": 1}))
        .await
        .expect("publish");
    photon
        .publish(topic_name, None, actor_json.clone(), json!({"value": 2}))
        .await
        .expect("publish");
    photon
        .publish(topic_name, None, actor_json.clone(), json!({"value": 3}))
        .await
        .expect("publish");

    let mut stream = photon.subscribe(topic_name, None, Some(2));
    let received = timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timeout")
        .expect("stream item")
        .expect("event ok");
    assert_eq!(
        received
            .payload_json
            .get("value")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        3,
        "subscribe with after_seq should replay only events after that seq"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn build_photon_runtime_installs_system_valence() {
    let _runtime = common::test_photon_runtime();
    let valence = photon_valence_identity::system_valence("integration_ok")
        .expect("system valence after build_photon_runtime");
    let _ = valence;
}

#[tokio::test(flavor = "multi_thread")]
async fn subscribe_none_filter_receives_keyed_events_local() {
    common::ensure_photon_test_env();
    let photon = Arc::new(Photon::builder().auto_registry().build().expect("build"));

    let topic_name = "test.keyed.unfiltered";
    let actor_json = json!({"System": {"operation": "test"}});

    let mut stream = photon.subscribe(topic_name, None, None);

    let photon_clone = Arc::clone(&photon);
    let actor_clone = actor_json.clone();
    let event_id = tokio::spawn(async move {
        photon_clone
            .publish(
                topic_name,
                Some("partition-a"),
                actor_clone,
                json!({"k": 1}),
            )
            .await
    })
    .await
    .expect("join handle")
    .expect("publish task");

    let received = timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timeout")
        .expect("stream item")
        .expect("event ok");

    assert_eq!(received.event_id, event_id);
    assert_eq!(received.topic_name, topic_name);
    assert_eq!(received.topic_key.as_deref(), Some("partition-a"));
}
