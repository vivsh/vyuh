mod conf;
mod site;

extern crate self as vyuh;

pub mod apidocs;
pub mod assets;
#[path = "auth/mod.rs"]
pub mod auth;
pub mod bundles;
pub mod callables;
pub mod channels;
pub mod collectors;
pub mod commands;
pub mod console;
pub mod db;
mod db_notify;
pub mod email;
pub mod embed;
pub mod emitters;
pub mod errors;
pub mod file_storage;
pub mod logging;
#[cfg(feature = "mcp")]
pub mod mcp;
pub mod middlewares;
pub(crate) mod notifiers;
pub mod observability;
pub mod prelude;
pub(crate) mod roles;
pub(crate) mod schedulers;
pub mod services;
pub mod signals;

pub mod tasks;
pub mod templates;
pub mod testing;
pub mod utils;
pub mod validation;
pub mod validators;
mod watch;

pub mod routes;
mod schema_assets;
pub use callables::{Data, DataValue, Operation, OperationId, OperationKind, Operations};
pub use commands::CommandError;
pub use conf::{DeploymentMode, SiteConf};
pub use console::{CONSOLE_AUDIENCE, ConsoleAccess};
pub use errors::{
    Error, ErrorCommandContext, ErrorContext, ErrorKind, ErrorRenderContext, ErrorRenderTarget,
    ErrorReport, ErrorRequestContext, ErrorSourceKind, ErrorView, HttpErrorRenderMode,
};
pub use file_storage::{
    FileStorageError, LocalStorage, SavedFile, StorageBackend, StorageName, UploadConf,
};
pub use serde;
pub use site::{Site, SiteConfig, SiteError};
pub use validation::{
    Valid, ValidRejection, Validate, ValidationError, ValidationReport, ValidationSchema,
};
pub use vyuh_macros::{MultipartData, test};
