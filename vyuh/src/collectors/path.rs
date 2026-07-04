use std::path::{Component, Path, PathBuf};

use super::error::{StaticAssetError, StaticExportError};

pub(crate) fn apply_prefix(prefix: &str, path: &str) -> String {
    let prefix = prefix.trim_end_matches('/');
    if path == "/" {
        format!("{prefix}/")
    } else {
        format!("{prefix}{path}")
    }
}

pub(crate) fn validate_url(path: &str) -> Result<(), StaticExportError> {
    if !path.starts_with('/') {
        return invalid(path, "URL must start with '/'");
    }
    if path.contains('?') {
        return invalid(path, "query strings are not supported");
    }
    if path.contains('#') {
        return invalid(path, "fragments are not supported");
    }
    if path.contains('\\') {
        return invalid(path, "backslashes are not allowed");
    }
    if path.contains('\0') {
        return invalid(path, "NUL bytes are not allowed");
    }
    for segment in path.split('/') {
        if segment == ".." {
            return invalid(path, "parent directory segments are not allowed");
        }
        if segment.ends_with(':') {
            return invalid(path, "Windows drive prefixes are not allowed");
        }
    }
    Ok(())
}

pub(crate) fn url_to_output_path(
    output_dir: &Path,
    url: &str,
) -> Result<PathBuf, StaticExportError> {
    validate_url(url)?;
    let rel = url_to_relative_path(url)?;
    safe_join(output_dir, &rel).map_err(|()| StaticExportError::OutputEscape {
        url: url.to_string(),
    })
}

pub(crate) fn url_to_relative_path(url: &str) -> Result<PathBuf, StaticExportError> {
    validate_url(url)?;
    let stripped = url.trim_start_matches('/');
    if stripped.is_empty() {
        return Ok(PathBuf::from("index.html"));
    }
    if url.ends_with('/') {
        let base = stripped.trim_end_matches('/');
        return Ok(Path::new(base).join("index.html"));
    }
    let path = Path::new(stripped);
    if path.extension().is_some() {
        return Ok(path.to_path_buf());
    }
    let mut rel = path.to_path_buf();
    rel.set_extension("html");
    Ok(rel)
}

pub(crate) fn asset_relative_path(path: &Path) -> Result<PathBuf, StaticAssetError> {
    let rel = path.to_string_lossy().replace('\\', "/");
    let Some(stripped) = rel.strip_prefix("public/") else {
        return Err(StaticAssetError::NonPublicAsset { path: rel });
    };
    safe_relative(stripped).map_err(|()| StaticAssetError::OutputEscape {
        path: stripped.to_string(),
    })
}

pub(crate) fn safe_join(base: &Path, rel: &Path) -> Result<PathBuf, ()> {
    let rel = safe_relative_path(rel)?;
    Ok(base.join(rel))
}

fn safe_relative(value: &str) -> Result<PathBuf, ()> {
    safe_relative_path(Path::new(value))
}

fn safe_relative_path(path: &Path) -> Result<PathBuf, ()> {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return Err(()),
        }
    }
    Ok(out)
}

fn invalid<T>(url: &str, reason: &'static str) -> Result<T, StaticExportError> {
    Err(StaticExportError::InvalidUrl {
        url: url.to_string(),
        reason,
    })
}
