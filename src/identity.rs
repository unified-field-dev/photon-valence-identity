//! Bridge [`photon_core::IdentityFactory`] to Valence.
//!
//! [`ValenceIdentityFactory`] is the piece that lets a `#[photon::subscribe]` handler take a
//! [`valence::Valence`] parameter: it deserializes the `actor_json` Photon captured at publish
//! time, hands it to a [`ValenceFactory`] to build a session, and wraps the outcome in an opaque
//! [`photon_core::Actor`] the handler's downcast helpers can recover.
//!
//! # Examples
//!
//! ```rust,no_run
//! use std::sync::Arc;
//!
//! use photon_valence_identity::{ProcessValenceFactory, ValenceIdentityFactory};
//! use valence::{install_default_mem_router, DEFAULT_IN_MEMORY_ROUTER_KEY};
//!
//! let router = install_default_mem_router();
//! let valence_factory = ProcessValenceFactory::arc(router, DEFAULT_IN_MEMORY_ROUTER_KEY);
//! let identity_factory = ValenceIdentityFactory::new(Arc::clone(&valence_factory));
//! # let _ = identity_factory;
//! ```
//!
//! Runnable: `cargo run -p photon-valence-identity --example wire_factory`

use std::sync::Arc;

use photon_core::{Actor as CoreActor, IdentityError, IdentityFactory};
use valence::{Actor, ValenceFactory};

/// Wraps a [`ValenceFactory`] as a Photon [`IdentityFactory`].
///
/// Construct with [`ValenceIdentityFactory::new`] and pass it to upstream Photon's
/// `start_executor`, or use this crate's [`start_executor`](crate::start_executor) /
/// [`build_photon_runtime`](crate::build_photon_runtime), which build one for you.
///
/// # Examples
///
/// ```rust,no_run
/// use std::sync::Arc;
///
/// use photon_valence_identity::{ProcessValenceFactory, ValenceIdentityFactory};
/// use valence::{install_default_mem_router, DEFAULT_IN_MEMORY_ROUTER_KEY};
///
/// let router = install_default_mem_router();
/// let valence_factory = ProcessValenceFactory::arc(router, DEFAULT_IN_MEMORY_ROUTER_KEY);
/// let identity_factory = ValenceIdentityFactory::new(valence_factory);
/// # let _ = identity_factory;
/// ```
///
/// See also `cargo run -p photon-valence-identity --example wire_factory`.
pub struct ValenceIdentityFactory {
    inner: Arc<dyn ValenceFactory>,
}

impl ValenceIdentityFactory {
    /// Wrap a [`ValenceFactory`] so it can be handed to Photon as an
    /// [`IdentityFactory`].
    ///
    /// Prefer a factory built with
    /// [`crate::router_config_reject_external_system`] / [`crate::ProcessValenceFactory::new`]
    /// so System-shaped client JSON is rejected.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use std::sync::Arc;
    ///
    /// use photon_valence_identity::ValenceIdentityFactory;
    ///
    /// # fn boot(
    /// #     photon: &photon::Photon,
    /// #     valence_factory: Arc<dyn valence::ValenceFactory>,
    /// # ) -> photon::Result<()> {
    /// let identity_factory = ValenceIdentityFactory::new(valence_factory);
    /// photon.start_executor(Arc::new(identity_factory))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(inner: Arc<dyn ValenceFactory>) -> Self {
        Self { inner }
    }
}

struct ValenceActor {
    label: String,
}

impl CoreActor for ValenceActor {
    fn label(&self) -> &str {
        &self.label
    }
    photon_core::actor_downcast_methods!();
}

impl IdentityFactory for ValenceIdentityFactory {
    /// Reconstruct a Photon [`Actor`] from publish-time `actor_json`.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError::InvalidActor`] when `actor_json` is not valid JSON, does not
    /// deserialize to [`valence::Actor`], or when the inner [`ValenceFactory::build`] fails
    /// (including System rejection on external-trust factories).
    fn reconstruct(&self, actor_json: &str) -> Result<Box<dyn CoreActor>, IdentityError> {
        let value: serde_json::Value = serde_json::from_str(actor_json)
            .map_err(|e| IdentityError::InvalidActor(e.to_string()))?;
        let actor: Actor = serde_json::from_value(value.clone())
            .map_err(|e| IdentityError::InvalidActor(e.to_string()))?;
        self.inner
            .build(&value)
            .map_err(|e| IdentityError::InvalidActor(e.to_string()))?;
        let label = format!("{actor:?}");
        Ok(Box::new(ValenceActor { label }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProcessValenceFactory;
    use photon_core::{IdentityError, IdentityFactory};
    use valence::{install_default_mem_router, DEFAULT_IN_MEMORY_ROUTER_KEY};

    fn mem_factory_external() -> Arc<dyn ValenceFactory> {
        ProcessValenceFactory::arc(install_default_mem_router(), DEFAULT_IN_MEMORY_ROUTER_KEY)
    }

    fn mem_factory_internal() -> Arc<dyn ValenceFactory> {
        ProcessValenceFactory::arc_internal(
            install_default_mem_router(),
            DEFAULT_IN_MEMORY_ROUTER_KEY,
        )
    }

    struct FailValenceFactory;

    impl ValenceFactory for FailValenceFactory {
        fn build(&self, _actor_json: &serde_json::Value) -> valence::Result<valence::Valence> {
            Err(valence::Error::Identity("factory build failed".into()))
        }
    }

    #[test]
    fn reconstruct_rejects_invalid_json() {
        let factory = ValenceIdentityFactory::new(mem_factory_external());
        match factory.reconstruct("not-json") {
            Ok(_) => panic!("expected invalid json"),
            Err(IdentityError::InvalidActor(msg)) => assert!(!msg.is_empty()),
            Err(other) => panic!("expected InvalidActor, got {other:?}"),
        }
    }

    #[test]
    fn reconstruct_rejects_invalid_actor_shape() {
        let factory = ValenceIdentityFactory::new(mem_factory_external());
        match factory.reconstruct(r#"{"not_an_actor":true}"#) {
            Ok(_) => panic!("expected invalid actor"),
            Err(IdentityError::InvalidActor(msg)) => assert!(!msg.is_empty()),
            Err(other) => panic!("expected InvalidActor, got {other:?}"),
        }
    }

    #[test]
    fn reconstruct_rejects_system_on_external_factory() {
        let factory = ValenceIdentityFactory::new(mem_factory_external());
        let actor_json = serde_json::to_string(&Actor::System {
            operation: "test".into(),
        })
        .expect("serialize");
        match factory.reconstruct(&actor_json) {
            Ok(_) => panic!("System must be rejected"),
            Err(IdentityError::InvalidActor(msg)) => assert!(msg.contains("System")),
            Err(other) => panic!("expected InvalidActor, got {other:?}"),
        }
    }

    #[test]
    fn reconstruct_maps_valence_build_failure() {
        let factory = ValenceIdentityFactory::new(Arc::new(FailValenceFactory));
        let actor_json = serde_json::to_string(&Actor::System {
            operation: "test".into(),
        })
        .expect("serialize");
        match factory.reconstruct(&actor_json) {
            Ok(_) => panic!("build should fail"),
            Err(IdentityError::InvalidActor(msg)) => {
                assert!(msg.contains("factory build failed"));
            }
            Err(other) => panic!("expected InvalidActor, got {other:?}"),
        }
    }

    #[test]
    fn reconstruct_ok_with_system_actor_internal() {
        let factory = ValenceIdentityFactory::new(mem_factory_internal());
        let actor_json = serde_json::to_string(&Actor::System {
            operation: "test".into(),
        })
        .expect("serialize");
        let actor = factory.reconstruct(&actor_json).expect("reconstruct");
        assert!(!actor.label().is_empty());
    }
}
