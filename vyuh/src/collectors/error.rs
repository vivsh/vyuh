use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum StaticExportError {
    #[error("invalid static export options: {0}")]
    InvalidExportOptions(&'static str),
    #[error("invalid URL '{url}': {reason}")]
    InvalidUrl { url: String, reason: &'static str },
    #[error("URL '{url}' escapes the output directory")]
    OutputEscape { url: String },
    #[error("URL '{left}' and '{right}' both map to '{path}'")]
    DuplicateOutputPath {
        left: String,
        right: String,
        path: PathBuf,
    },
    #[error("route '{url}' returned status {status}")]
    NonSuccess {
        url: String,
        status: axum::http::StatusCode,
    },
    #[error("failed to render '{url}': {source}")]
    RenderFailed {
        url: String,
        #[source]
        source: crate::Error,
    },
    #[error("failed to collect URLs: {0}")]
    UrlProvider(#[from] crate::Error),
    #[error("failed to collect assets: {0}")]
    Asset(#[from] StaticAssetError),
    #[error("filesystem error at '{path}': {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum StaticAssetError {
    #[error("invalid asset collection options: {0}")]
    InvalidOptions(&'static str),
    #[error("asset path '{path}' is outside public assets")]
    NonPublicAsset { path: String },
    #[error("asset path '{path}' escapes the output directory")]
    OutputEscape { path: String },
    #[error("asset path '{path}' conflicts with rendered page '{page}'")]
    PageConflict { path: PathBuf, page: PathBuf },
    #[error("filesystem error at '{path}': {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}
