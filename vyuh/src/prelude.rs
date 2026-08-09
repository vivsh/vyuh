//! Common imports for ordinary Vyuh application code.
//!
//! The prelude contains symbols most handlers and examples need directly.
//! Registration APIs stay under the `bundles::` namespace:
//!
//! ```rust
//! use vyuh::prelude::*;
//!
//! #[bundles::route(path = "/health")]
//! async fn health() -> Json<&'static str> {
//!     Json("ok")
//! }
//! ```

// ── Core framework types ────────────────────────────────────────────────────

pub use crate::{
    Data, DeploymentMode, Error, OperationId, Site, SiteConf, SiteError, Valid, Validate,
};

pub use serde::{Deserialize, Serialize};

// ── Registration namespace ──────────────────────────────────────────────────

pub use crate::bundles;
pub use crate::db;
#[cfg(feature = "migrations")]
pub use crate::db::embed_migrations;
pub use crate::db::{Model, PgNotifyDbExt, Record};
pub use crate::embed::embed_assets;
pub use crate::observability::ObservabilityConf;

// ── Routes ──────────────────────────────────────────────────────────────────

pub use crate::routes::{
    Accepted, AppendHeaders, ClientIp, Created, FileResponse, Form, Html, HttpMethod, IntoResponse,
    Json, NoContent, POLL, Path, PermanentRedirect, Query, Redirect, SSE, State, StatusCode,
    StreamResponse, Subscriber, TemporaryRedirect, WS, redirect,
};

// ── Tasks ───────────────────────────────────────────────────────────────────

pub use crate::tasks::{
    Continuation, DEFAULT_TASK_LANE, TaskId, TaskLane, TaskOptions, TaskReceipt, TaskState,
};

// ── Services ─────────────────────────────────────────────────────────────────

pub use crate::services::ServiceRef;

// ── Channels ─────────────────────────────────────────────────────────────────

pub use crate::channels::{ChannelResponse, Channels, UserKey};

// ── Cache ───────────────────────────────────────────────────────────────────

pub use crate::cache::{CacheName, CacheTtl, DEFAULT_CACHE};

// ── Authentication ──────────────────────────────────────────────────────────

pub use crate::auth::{AuthUser, Permit, Scope, ScopeExpr, ScopeRule};
