//! Small framework-neutral utilities used by many web applications.
//!
//! This module is intentionally conservative. Subsystem-specific conveniences
//! belong to their owning modules such as `routes`, `db`, `auth`, or `errors`.

pub mod debounce;
pub mod html;
pub mod text;
