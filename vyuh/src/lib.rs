mod conf;
mod site;

extern crate self as vyuh;

pub mod apidocs;
pub mod assets;
pub mod auth;
pub mod bundles;
pub mod callables;
pub mod channels;
pub mod collectors;
pub mod commands;
pub mod console;
pub mod db;
mod db_notify;
pub mod embed;
pub mod emitters;
pub mod errors;
pub mod file_storage;
pub mod logging;
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

#[cfg(test)]
mod testing_tests;

pub mod routes;
pub use callables::{Data, DataValue, Operation, OperationKind};
pub use commands::CommandError;
pub use conf::{DeploymentMode, SiteConf};
pub use errors::{
    Error, ErrorCommandContext, ErrorContext, ErrorKind, ErrorRenderContext, ErrorRenderTarget,
    ErrorReport, ErrorRequestContext, ErrorSourceKind, ErrorView, HttpErrorRenderMode,
};
pub use file_storage::{
    FileStorageError, LocalStorage, SavedFile, StorageBackend, StorageName, UploadConf,
};
pub use schemars;
pub use serde;
pub use site::{Site, SiteConfig, SiteError};
pub use validation::{
    Valid, ValidRejection, Validate, ValidationError, ValidationReport, ValidationSchema,
};
pub use vyuh_macros::MultipartData;
