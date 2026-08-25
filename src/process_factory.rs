//! Process-global [`ValenceFactory`] from a pinned [`valence::DatabaseRouter`].
//!
//! [`ProcessValenceFactory`] pins a [`valence::RouterValenceFactory`] for the process lifetime.
//!
//! ## Security
//!
//! [`ProcessValenceFactory::new`] installs [`RejectExternalSystemActor`] at
//! [`ActorTrust::External`] so client-captured `actor_json` cannot mint
//! [`valence::Actor::System`] at handler dispatch. Use
//! [`ProcessValenceFactory::new_internal`] for background [`crate::system_valence()`]
//! paths that legitimately need System.
//!
//! # Examples
//!
//! ```rust,no_run
//! use photon_valence_identity::ProcessValenceFactory;
//! use valence::{install_default_mem_router, DEFAULT_IN_MEMORY_ROUTER_KEY};
//!
//! let router = install_default_mem_router();
//! let factory = ProcessValenceFactory::new(router, DEFAULT_IN_MEMORY_ROUTER_KEY);
//! # let _ = factory;
//! ```
//!
//! Runnable: `cargo run -p photon-valence-identity --example wire_factory`

use std::sync::Arc;

use valence::{
    ActorTrust, DatabaseRouter, RejectExternalSystemActor, RouterValenceFactory,
    RouterValenceFactoryConfig, Valence, ValenceFactory,
};

/// Router config that rejects System-shaped actors on the default external trust path.
///
/// Use for any factory that may receive client-supplied `actor_json`. For in-process System
/// workers, clone and set [`valence::ActorTrust::Internal`] on the returned config.
///
/// Crate guide: [External-safe router config](crate#external-safe-router-config).
///
/// # Examples
///
/// ```rust,ignore
/// use photon_core::IdentityError;
/// use photon_valence_identity::{
///     router_config_reject_external_system, ValenceIdentityFactory,
/// };
/// use valence::{install_default_mem_router, RouterValenceFactory, DEFAULT_IN_MEMORY_ROUTER_KEY};
///
/// let router = install_default_mem_router();
/// let config = router_config_reject_external_system(DEFAULT_IN_MEMORY_ROUTER_KEY);
/// let valence_factory = RouterValenceFactory::arc(router, config);
/// let identity = ValenceIdentityFactory::new(valence_factory);
/// let system = r#"{"System":{"operation":"probe"}}"#;
/// match identity.reconstruct(system) {
///     Ok(_) => panic!("System must be rejected on external trust"),
///     Err(IdentityError::InvalidActor(msg)) => assert!(msg.contains("System")),
/// }
/// ```
#[must_use]
pub fn router_config_reject_external_system(
    default_backend_key: impl Into<String>,
) -> RouterValenceFactoryConfig {
    RouterValenceFactoryConfig::new(default_backend_key)
        .actor_json_policy(RejectExternalSystemActor)
}

/// [`ValenceFactory`] backed by the process-global router handle.
///
/// Use this when your process keeps exactly one [`DatabaseRouter`] alive for its whole lifetime
/// (the common case for `mem` / single-tenant hosts) and just needs a default backend key applied
/// on every reconstructed [`Valence`]. Hosts with per-tenant or per-request wiring should build
/// [`RouterValenceFactory`] (or a custom [`ValenceFactory`]) directly instead.
///
/// # Examples
///
/// ```rust,no_run
/// use photon_valence_identity::ProcessValenceFactory;
/// use valence::{install_default_mem_router, DEFAULT_IN_MEMORY_ROUTER_KEY};
///
/// let router = install_default_mem_router();
/// let factory = ProcessValenceFactory::new(router, DEFAULT_IN_MEMORY_ROUTER_KEY);
/// # let _ = factory;
/// ```
#[derive(Clone)]
pub struct ProcessValenceFactory {
    inner: RouterValenceFactory,
}

impl ProcessValenceFactory {
    /// Wrap `router` with external System rejection (safe default for publish/dispatch).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use photon_valence_identity::ProcessValenceFactory;
    /// use valence::{install_default_mem_router, DEFAULT_IN_MEMORY_ROUTER_KEY};
    ///
    /// let router = install_default_mem_router();
    /// let factory = ProcessValenceFactory::new(router, DEFAULT_IN_MEMORY_ROUTER_KEY);
    /// # let _ = factory;
    /// ```
    pub fn new(router: Arc<DatabaseRouter>, default_backend_key: impl Into<String>) -> Self {
        Self {
            inner: RouterValenceFactory::new(
                router,
                router_config_reject_external_system(default_backend_key),
            ),
        }
    }

    /// Like [`Self::new`], but allows System actors (`ActorTrust::Internal`).
    ///
    /// Use for [`crate::system_valence()`] / platform background work — **not** for
    /// dispatching client-captured publish `actor_json`.
    pub fn new_internal(
        router: Arc<DatabaseRouter>,
        default_backend_key: impl Into<String>,
    ) -> Self {
        let mut config = router_config_reject_external_system(default_backend_key);
        config.actor_trust = ActorTrust::Internal;
        Self {
            inner: RouterValenceFactory::new(router, config),
        }
    }

    /// Build a [`ProcessValenceFactory`] and return it as a boxed [`ValenceFactory`], ready for
    /// dependency injection into [`crate::build_photon_runtime`] or
    /// [`crate::ValenceIdentityFactory::new`].
    pub fn arc(
        router: Arc<DatabaseRouter>,
        default_backend_key: impl Into<String>,
    ) -> Arc<dyn ValenceFactory> {
        Arc::new(Self::new(router, default_backend_key))
    }

    /// Internal-trust boxed factory for [`crate::system_valence()`].
    pub fn arc_internal(
        router: Arc<DatabaseRouter>,
        default_backend_key: impl Into<String>,
    ) -> Arc<dyn ValenceFactory> {
        Arc::new(Self::new_internal(router, default_backend_key))
    }
}

impl ValenceFactory for ProcessValenceFactory {
    /// Build a [`Valence`] from serialized actor JSON.
    ///
    /// # Errors
    ///
    /// Returns [`valence::Error`] when `actor_json` is malformed, fails actor policy (for example
    /// [`valence::Actor::System`] on external-trust factories), or router construction fails.
    fn build(&self, actor_json: &serde_json::Value) -> valence::Result<Valence> {
        self.inner.build(actor_json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use valence::{install_default_mem_router, Actor, DEFAULT_IN_MEMORY_ROUTER_KEY};

    #[test]
    fn external_rejects_invalid_and_system_actor_json() {
        let router = install_default_mem_router();
        let factory = ProcessValenceFactory::new(router, DEFAULT_IN_MEMORY_ROUTER_KEY);
        assert!(factory
            .build(&serde_json::json!({"not_an_actor": true}))
            .is_err());
        let system = serde_json::to_value(Actor::System {
            operation: "test".into(),
        })
        .expect("serialize");
        let err = factory.build(&system).expect_err("System rejected");
        assert!(err.to_string().contains("System"));
    }

    #[test]
    fn internal_accepts_system_actor() {
        let router = install_default_mem_router();
        let factory = ProcessValenceFactory::arc_internal(router, DEFAULT_IN_MEMORY_ROUTER_KEY);
        let actor_json = serde_json::to_value(Actor::System {
            operation: "test".into(),
        })
        .expect("serialize");
        let valence = factory.build(&actor_json).expect("build");
        let _ = valence;
    }

    #[test]
    fn external_accepts_user_actor() {
        let router = install_default_mem_router();
        let factory = ProcessValenceFactory::arc(router, DEFAULT_IN_MEMORY_ROUTER_KEY);
        let actor_json = serde_json::to_value(Actor::User {
            user_id: "u1".into(),
        })
        .expect("serialize");
        let valence = factory.build(&actor_json).expect("build");
        let _ = valence;
    }
}
