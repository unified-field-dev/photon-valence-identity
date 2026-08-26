//! Reconstructs Valence sessions for [Photon] handlers and runs the local executor.
//!
//! [Photon] captures opaque `actor_json` at publish time and asks a host-supplied
//! [`photon_core::IdentityFactory`] to turn that blob back into a [`photon_core::Actor`] when a
//! `#[photon::subscribe]` handler runs. This crate implements that factory for hosts that already
//! use [Valence] as their permission-checked data layer: handlers receive a live [`Valence`]
//! session instead of re-resolving claims from raw JSON.
//!
//! On top of identity, the crate ships a local **executor** that discovers
//! `#[photon::subscribe]` handlers, subscribes to their topics, and dispatches each event through
//! a fresh [`Valence`] built from the event's actor, plus [`build_photon_runtime`] for one-call
//! boot wiring when you want all of that together.
//!
//! [Photon]: https://github.com/unified-field-dev/photon
//! [Valence]: https://github.com/unified-field-dev/valence
//!
//! ## Features
//!
//! - **Photon runtime wiring** — Wires Photon, pins the process [`ValenceFactory`], and starts the
//!   handler executor in one boot call so `#[photon::subscribe]` handlers begin dispatching
//!   immediately. [Get started](#build-photon-runtime).
//!   API reference: [`build_photon_runtime`], [`PhotonRuntime`].
//! - **Valence identity factory** — Implements Photon's [`photon_core::IdentityFactory`] from an
//!   existing [`ValenceFactory`] when you bring your own Photon or executor wiring.
//!   [Get started](#valence-identity-factory).
//!   API reference: [`ValenceIdentityFactory`].
//! - **Handler executor** — Discovers `#[photon::subscribe]` handlers via [`HandlerRegistry`] and
//!   runs them with backpressure, dead-letter queueing, and durable checkpoints.
//!   [Get started](#start-executor).
//!   API reference: [`start_executor`], [`HandlerRegistry`], [`ExecutorHandle`].
//! - **System valence** — Builds a System-scoped [`Valence`] for background work outside any
//!   single subscribed event (retention sweeps, startup jobs). [Get started](#system-valence).
//!   API reference: [`system_valence()`], [`set_process_system_valence_factory`].
//! - **Process Valence factory** — Wraps a pinned process-global [`valence::DatabaseRouter`]
//!   behind [`ValenceFactory`] for single-router hosts. [Get started](#process-valence-factory).
//!   API reference: [`ProcessValenceFactory`].
//! - **External-safe router config** — Rejects client-supplied System actor JSON on the default
//!   external trust path before dispatch can mint privileged sessions.
//!   [Get started](#external-safe-router-config).
//!   API reference: [`router_config_reject_external_system`].
//!
//! ## Getting started
//!
//! Build an in-memory router, call [`build_photon_runtime`], and keep the returned
//! [`PhotonRuntime`] alive for the process lifetime:
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use valence::{install_default_mem_router, RouterValenceFactory, RouterValenceFactoryConfig};
//! use valence::DEFAULT_IN_MEMORY_ROUTER_KEY;
//!
//! # fn main() -> anyhow::Result<()> {
//! let router = install_default_mem_router();
//! let valence_factory = RouterValenceFactory::arc(
//!     router,
//!     RouterValenceFactoryConfig::new(DEFAULT_IN_MEMORY_ROUTER_KEY),
//! );
//! let runtime = photon_valence_identity::build_photon_runtime(&valence_factory)?;
//! assert!(Arc::strong_count(&runtime.photon) >= 1);
//! # Ok(())
//! # }
//! ```
//!
//! Hosts that call upstream Photon's `start_executor` directly can wire
//! [`ValenceIdentityFactory`] instead — see [Valence identity factory](#valence-identity-factory).
//!
//! # Build Photon runtime
//!
//! [`build_photon_runtime`] is the one-call entry point most hosts use at process boot. It pins
//! your [`ValenceFactory`] for handler dispatch and [`system_valence()`], builds Photon with
//! auto-discovered topics, and starts this crate's executor so registered `#[photon::subscribe]`
//! handlers begin running immediately.
//!
//! **Prerequisites:** a [`ValenceFactory`] (typically [`ProcessValenceFactory::arc`] or
//! `valence::RouterValenceFactory` with [`router_config_reject_external_system`] for external
//! publish paths), plus valid Photon transport/storage env if you override defaults.
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use photon_valence_identity::build_photon_runtime;
//! use valence::{install_default_mem_router, RouterValenceFactory, RouterValenceFactoryConfig};
//! use valence::DEFAULT_IN_MEMORY_ROUTER_KEY;
//!
//! # fn main() -> anyhow::Result<()> {
//! let router = install_default_mem_router();
//! let valence_factory = RouterValenceFactory::arc(
//!     router,
//!     RouterValenceFactoryConfig::new(DEFAULT_IN_MEMORY_ROUTER_KEY),
//! );
//! let runtime = build_photon_runtime(&valence_factory)?;
//! assert!(Arc::strong_count(&runtime.photon) >= 1);
//! # Ok(())
//! # }
//! ```
//!
//! **Outcome:** [`PhotonRuntime`] holds a configured [`photon_runtime::Photon`] plus an
//! [`ExecutorHandle`]; dropping `runtime.executor` aborts handler dispatch.
//!
//! **Next:** [Start executor](#start-executor) when you assemble Photon parts yourself, or
//! [System valence](#system-valence) for background jobs outside subscribed events.
//!
//! # Valence identity factory
//!
//! [`ValenceIdentityFactory`] is the identity-only path when you already run Photon (or another
//! executor) and only need Photon's [`photon_core::IdentityFactory`] implemented from your
//! [`ValenceFactory`]. Hand it to upstream `Photon::start_executor` or use it inside custom wiring
//! so `#[photon::subscribe]` handlers can take a [`Valence`] parameter.
//!
//! **Prerequisites:** a [`ValenceFactory`] that can rebuild sessions from publish-time actor JSON
//! (see [Process Valence factory](#process-valence-factory) for the common single-router case).
//!
//! ```rust,no_run
//! use std::sync::Arc;
//!
//! use photon_core::IdentityFactory;
//! use photon_valence_identity::{ProcessValenceFactory, ValenceIdentityFactory};
//! use valence::{install_default_mem_router, Actor, DEFAULT_IN_MEMORY_ROUTER_KEY};
//!
//! # fn main() -> anyhow::Result<()> {
//! let router = install_default_mem_router();
//! let valence_factory = ProcessValenceFactory::arc(router, DEFAULT_IN_MEMORY_ROUTER_KEY);
//! let identity_factory = ValenceIdentityFactory::new(Arc::clone(&valence_factory));
//! let user = Actor::User {
//!     user_id: "identity-factory".into(),
//! };
//! let actor = identity_factory.reconstruct(&serde_json::to_string(&user)?)?;
//! assert!(actor.label().contains("User"));
//! # Ok(())
//! # }
//! ```
//!
//! **Outcome:** [`ValenceIdentityFactory`] via [`photon_core::IdentityFactory::reconstruct`] returns a Photon [`photon_core::Actor`] whose
//! label reflects the deserialized Valence actor; the inner [`ValenceFactory::build`] ran
//! successfully.
//!
//! **Variant:** pass `Arc::new(identity_factory)` to upstream `photon.start_executor(...)` when
//! you skip [`build_photon_runtime`].
//!
//! **Next:** [Build Photon runtime](#build-photon-runtime) when you want executor + Photon wiring
//! in one call.
//!
//! # Start executor
//!
//! [`start_executor`] discovers every `#[photon::subscribe]` handler through
//! [`HandlerRegistry::auto_discover`], opens one Photon subscription per distinct topic/key pair,
//! and dispatches matching events with a fresh [`Valence`] per event. Call it after Photon parts
//! are built, typically at boot alongside pinning a process [`ValenceFactory`].
//!
//! **Prerequisites:** configured [`photon_runtime::Photon`], a [`ValenceFactory`], and
//! [`photon_backend::ExecutorServices`] from `photon_runtime::runtime::build_photon_parts` (or
//! equivalent). [`build_photon_runtime`] calls this for you.
//!
//! ```rust,ignore
//! use photon_valence_identity::{start_executor, HandlerRegistry};
//!
//! # fn boot(
//! #     photon: &std::sync::Arc<photon_runtime::Photon>,
//! #     valence_factory: &std::sync::Arc<dyn valence::ValenceFactory>,
//! #     services: &std::sync::Arc<photon_backend::ExecutorServices>,
//! # ) {
//! let registry = HandlerRegistry::auto_discover();
//! let handle = start_executor(photon, valence_factory, services);
//! if registry.is_empty() {
//!     assert_eq!(registry.topic_subscription_keys().len(), 0);
//! } else {
//!     assert!(!registry.topic_subscription_keys().is_empty());
//! }
//! handle.abort();
//! # }
//! ```
//!
//! **Outcome:** an [`ExecutorHandle`] owning subscription tasks; [`HandlerRegistry`] lists every
//! linked handler descriptor grouped by topic.
//!
//! **Failure:** returns [`ExecutorHandle::empty`] immediately when no handlers were discovered.
//!
//! **Next:** [Build Photon runtime](#build-photon-runtime) for the all-in-one boot path.
//!
//! # System valence
//!
//! [`system_valence()`] builds a System-scoped [`Valence`] for work that runs outside any single
//! subscribed event — retention sweeps, startup jobs, admin tasks. Install an internal-trust
//! factory with [`set_process_system_valence_factory`] before calling it so external publish
//! paths can keep System rejection enabled.
//!
//! **Prerequisites:** call [`set_process_system_valence_factory`] with
//! [`ProcessValenceFactory::arc_internal`] (or call [`build_photon_runtime`] / pin a dispatch
//! factory first). Background calls happen after boot, not inside `#[photon::subscribe]` bodies.
//!
//! ```rust,no_run
//! use photon_valence_identity::{
//!     set_process_system_valence_factory, system_valence, ProcessValenceFactory,
//! };
//! use valence::{install_default_mem_router, DEFAULT_IN_MEMORY_ROUTER_KEY};
//!
//! # fn main() -> anyhow::Result<()> {
//! let router = install_default_mem_router();
//! set_process_system_valence_factory(ProcessValenceFactory::arc_internal(
//!     router,
//!     DEFAULT_IN_MEMORY_ROUTER_KEY,
//! ));
//! let valence = system_valence("retention_sweep")?;
//! assert!(valence.active_backend().is_ok());
//! # Ok(())
//! # }
//! ```
//!
//! **Outcome:** a live System [`Valence`] scoped to the operation label you pass in.
//!
//! **Failure:** returns an error if no factory was installed, or if the factory rejects System on
//! external trust.
//!
//! **Next:** [Process Valence factory](#process-valence-factory) for the pinned-router wrapper.
//!
//! # Process Valence factory
//!
//! [`ProcessValenceFactory`] pins one [`valence::DatabaseRouter`] for the process lifetime and
//! exposes it as a [`ValenceFactory`]. Use it when your host keeps a single router alive (typical
//! for `mem` / single-tenant setups) and wants external System rejection on publish/dispatch paths
//! by default.
//!
//! **Prerequisites:** an installed [`valence::DatabaseRouter`] with at least one backend
//! registered (for example `valence::install_default_mem_router()`).
//!
//! ```rust,no_run
//! use photon_valence_identity::ProcessValenceFactory;
//! use valence::{install_default_mem_router, Actor, DEFAULT_IN_MEMORY_ROUTER_KEY};
//!
//! # fn main() -> anyhow::Result<()> {
//! let router = install_default_mem_router();
//! let factory = ProcessValenceFactory::arc(router, DEFAULT_IN_MEMORY_ROUTER_KEY);
//! let user = serde_json::to_value(&Actor::User {
//!     user_id: "process-factory".into(),
//! })?;
//! let valence = factory.build(&user)?;
//! assert!(valence.active_backend().is_ok());
//! # Ok(())
//! # }
//! ```
//!
//! **Outcome:** a [`Valence`] session backed by the pinned router and default backend key.
//!
//! **Variant:** [`ProcessValenceFactory::arc_internal`] for [`system_valence()`] paths that
//! legitimately need [`valence::Actor::System`].
//!
//! **Next:** [External-safe router config](#external-safe-router-config) when building
//! `RouterValenceFactory` directly.
//!
//! # External-safe router config
//!
//! [`router_config_reject_external_system`] installs [`valence::RejectExternalSystemActor`] on
//! [`valence::RouterValenceFactoryConfig`] so external publish/dispatch cannot mint
//! [`valence::Actor::System`]. Internal workers that legitimately need System set
//! [`valence::ActorTrust::Internal`] on the config instead (as [`ProcessValenceFactory::arc_internal`]
//! does).
//!
//! **Prerequisites:** a `valence::DatabaseRouter` with at least one backend registered.
//!
//! ```rust,ignore
//! use photon_core::{IdentityError, IdentityFactory};
//! use photon_valence_identity::{router_config_reject_external_system, ValenceIdentityFactory};
//! use valence::{install_default_mem_router, RouterValenceFactory, DEFAULT_IN_MEMORY_ROUTER_KEY};
//!
//! let router = install_default_mem_router();
//! let config = router_config_reject_external_system(DEFAULT_IN_MEMORY_ROUTER_KEY);
//! let valence_factory = RouterValenceFactory::arc(router, config);
//! let identity = ValenceIdentityFactory::new(valence_factory);
//! let system = r#"{"System":{"operation":"probe"}}"#;
//! match identity.reconstruct(system) {
//!     Ok(_) => panic!("System must be rejected on external trust"),
//!     Err(IdentityError::InvalidActor(msg)) => assert!(msg.contains("System")),
//! }
//! ```
//!
//! **Outcome:** System-shaped JSON fails closed with [`photon_core::IdentityError::InvalidActor`].
//!
//! **Next:** set `ActorTrust::Internal` on a config clone when an in-process worker must mint
//! System (see [System valence](#system-valence)).
//!
//! ## Examples
//!
//! | Level | Where | What |
//! |-------|-------|------|
//! | Highlight | Getting started above | In-memory router + [`build_photon_runtime`] |
//! | Mid | [Valence identity factory](#valence-identity-factory) | Identity-only wiring with upstream Photon |
//! | Detailed | `wire_factory` | User reconstruct, System reject, [`system_valence()`] |
//! | Detailed | `persist_actor_recover` | File-persisted actor JSON → executor Valence |
//!
//! ```bash
//! cargo run -p photon-valence-identity --example wire_factory
//! cargo run -p photon-valence-identity --example persist_actor_recover
//! ```
//!
//! Success (`wire_factory`): stderr prints `wire_factory: OK — User reconstruct + System reject + system_valence`.

pub mod executor;
mod handler_descriptor;
mod handler_registry;
pub mod identity;
pub mod process_factory;
pub mod runtime;
pub mod system_valence;
mod telemetry;

pub use executor::{start_executor, ExecutorHandle};
pub use handler_descriptor::{HandlerDescriptor, HandlerDispatch};
pub use handler_registry::HandlerRegistry;
pub use identity::ValenceIdentityFactory;
/// Backend-owned worker pool, DLQ, and checkpoint services, re-exported for callers that
/// build their own executor loop instead of using [`start_executor`].
pub use photon_backend::ExecutorServices;
pub use process_factory::{router_config_reject_external_system, ProcessValenceFactory};
/// Quark inventory, re-exported so downstream crates can register `#[photon::subscribe]`
/// handlers without depending on `uf-quark` directly.
pub use quark::inventory;
pub use runtime::{build_photon_runtime, PhotonRuntime, PhotonRuntimeParts};
pub use system_valence::{
    set_process_system_valence_factory, set_process_valence_factory, system_valence,
};
/// Core Valence types re-exported for convenience: an [`Actor`] identifies who is acting, a
/// [`Valence`] is the permission-checked session handlers receive, and [`ValenceFactory`]
/// reconstructs that session from captured actor JSON.
pub use valence::{Actor, Valence, ValenceFactory};
