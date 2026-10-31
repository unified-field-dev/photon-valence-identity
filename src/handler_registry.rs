//! Registry of subscription handlers discovered via `#[photon::subscribe]`.
//!
//! [`HandlerRegistry`] is the lookup table [`crate::start_executor`] dispatches from: one entry
//! per `#[photon::subscribe]` handler, indexed both by insertion order (for iteration) and by
//! `(topic_name, topic_key_filter)` (for grouping handlers into subscription streams).

use std::collections::HashMap;

use crate::handler_descriptor::HandlerDescriptor;

/// Key: (`topic_name`, `topic_key_filter` for subscription stream grouping).
type TopicKey = (String, Option<String>);

/// Registry of all handlers discovered via `#[photon::subscribe]`.
///
/// Build with [`HandlerRegistry::auto_discover`] to pick up every handler registered through
/// Quark inventory, or with [`HandlerRegistry::new`] + [`HandlerRegistry::register`] for a
/// hand-picked set (useful in tests).
///
/// # Examples
///
/// ```rust
/// use photon_valence_identity::HandlerRegistry;
///
/// let registry = HandlerRegistry::auto_discover();
/// // Handler count depends on which `#[photon::subscribe]` functions were linked in.
/// let _ = registry.len();
/// ```
#[derive(Debug, Clone, Default)]
pub struct HandlerRegistry {
    /// Handlers grouped by (`topic_name`, `topic_key_filter`) for executor dispatch.
    by_topic_and_key: HashMap<TopicKey, Vec<&'static HandlerDescriptor>>,
    /// All descriptors for iteration.
    all: Vec<&'static HandlerDescriptor>,
}

impl HandlerRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a registry populated from all `#[photon::subscribe]` descriptors.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use photon_valence_identity::HandlerRegistry;
    ///
    /// let registry = HandlerRegistry::auto_discover();
    /// // Handler count depends on which `#[photon::subscribe]` functions were linked in.
    /// let _ = registry.len();
    /// ```
    pub fn auto_discover() -> Self {
        let mut registry = Self::new();
        for descriptor in quark::inventory::iter::<HandlerDescriptor> {
            registry.register(descriptor);
        }
        registry
    }

    /// Register a handler descriptor.
    pub fn register(&mut self, descriptor: &'static HandlerDescriptor) {
        let key: TopicKey = (
            descriptor.topic_name.to_string(),
            descriptor.topic_key_filter.map(str::to_string),
        );
        self.by_topic_and_key
            .entry(key)
            .or_default()
            .push(descriptor);
        self.all.push(descriptor);
    }

    /// List unique (`topic_name`, `topic_key_filter`) pairs that have handlers.
    pub fn topic_subscription_keys(&self) -> Vec<(String, Option<String>)> {
        self.by_topic_and_key.keys().cloned().collect()
    }

    /// Get all handlers that match an event (`topic_name`, `topic_key`).
    pub fn handlers_for_event(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
    ) -> Vec<&'static HandlerDescriptor> {
        self.all
            .iter()
            .filter(|d| d.matches_event(topic_name, topic_key))
            .copied()
            .collect()
    }

    /// Get handlers for a topic and key filter (for a single subscription stream).
    pub fn handlers_for_topic_key(
        &self,
        topic_name: &str,
        topic_key_filter: Option<&str>,
    ) -> Vec<&'static HandlerDescriptor> {
        let key: TopicKey = (topic_name.to_string(), topic_key_filter.map(str::to_string));
        self.by_topic_and_key.get(&key).cloned().unwrap_or_default()
    }

    /// List all registered topic names (unique).
    pub fn topic_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.all.iter().map(|d| d.topic_name).collect();
        names.sort_unstable();
        names.dedup();
        names
    }

    /// Number of registered handlers.
    pub const fn len(&self) -> usize {
        self.all.len()
    }

    /// Check if the registry is empty.
    pub const fn is_empty(&self) -> bool {
        self.all.is_empty()
    }

    /// Iterate over all descriptors.
    pub fn iter(&self) -> impl Iterator<Item = &'static HandlerDescriptor> + '_ {
        self.all.iter().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler_descriptor::{HandlerDescriptor, HandlerDispatch};
    use photon_backend::{Event, Result, SubscriptionMode};
    use std::future::Future;
    use std::pin::Pin;

    fn noop_dispatch(
        _: valence::Valence,
        _: Event,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        Box::pin(async { Ok(()) })
    }

    #[test]
    fn new_is_empty() {
        let registry = HandlerRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert_eq!(registry.topic_names(), [] as [&str; 0]);
    }

    #[test]
    fn register_and_handlers_for_event() {
        let descriptor = Box::leak(Box::new(HandlerDescriptor::new(
            "test.handler.topic",
            Some("sub.one"),
            None,
            SubscriptionMode::Durable,
            noop_dispatch as HandlerDispatch,
        )));
        let mut registry = HandlerRegistry::new();
        registry.register(descriptor);
        assert_eq!(registry.len(), 1);
        assert_eq!(registry.topic_names(), vec!["test.handler.topic"]);
        let handlers = registry.handlers_for_event("test.handler.topic", None);
        assert_eq!(handlers.len(), 1);
        assert_eq!(handlers[0].topic_name, "test.handler.topic");
    }

    #[test]
    fn handlers_for_event_empty_when_no_match() {
        let descriptor = Box::leak(Box::new(HandlerDescriptor::new(
            "test.other",
            None,
            None,
            SubscriptionMode::Ephemeral,
            noop_dispatch as HandlerDispatch,
        )));
        let mut registry = HandlerRegistry::new();
        registry.register(descriptor);
        let handlers = registry.handlers_for_event("other.topic", None);
        assert!(handlers.is_empty());
    }

    #[test]
    fn handlers_for_event_empty_when_key_filter_mismatches() {
        let descriptor = Box::leak(Box::new(HandlerDescriptor::new(
            "test.keyed",
            Some("sub.keyed"),
            Some("key-a"),
            SubscriptionMode::Durable,
            noop_dispatch as HandlerDispatch,
        )));
        let mut registry = HandlerRegistry::new();
        registry.register(descriptor);

        assert!(registry
            .handlers_for_event("test.keyed", Some("key-b"))
            .is_empty());
        assert!(registry.handlers_for_event("test.keyed", None).is_empty());
        assert_eq!(
            registry
                .handlers_for_event("test.keyed", Some("key-a"))
                .len(),
            1
        );
    }

    #[test]
    fn handlers_for_topic_key_empty_when_subscription_key_mismatches() {
        let descriptor = Box::leak(Box::new(HandlerDescriptor::new(
            "test.stream",
            None,
            Some("partition-1"),
            SubscriptionMode::Ephemeral,
            noop_dispatch as HandlerDispatch,
        )));
        let mut registry = HandlerRegistry::new();
        registry.register(descriptor);

        assert!(registry
            .handlers_for_topic_key("test.stream", Some("partition-2"))
            .is_empty());
        assert!(registry
            .handlers_for_topic_key("test.stream", None)
            .is_empty());
        assert_eq!(
            registry
                .handlers_for_topic_key("test.stream", Some("partition-1"))
                .len(),
            1
        );
    }
}
