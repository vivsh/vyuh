use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use crate::{Site, embed::DirSet};

use super::{
    StaticAssetError,
    glob::GlobMatcher,
    path::{asset_relative_path, safe_join},
};

#[derive(Debug, Clone)]
pub struct CollectStaticOptions {
    pub output: PathBuf,
    pub clean: bool,
    pub glob: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CollectStaticReport {
    pub copied: usize,
    pub output: PathBuf,
}

impl CollectStaticOptions {
    pub fn new(output: impl Into<PathBuf>) -> Self {
        Self {
            output: output.into(),
            clean: false,
            glob: None,
        }
    }

    pub fn clean(mut self, clean: bool) -> Self {
        self.clean = clean;
        self
    }

    pub fn glob(mut self, glob: Option<String>) -> Self {
        self.glob = glob;
        self
    }
}

pub async fn collect_assets(
    site: &Site,
    options: CollectStaticOptions,
) -> Result<CollectStaticReport, StaticAssetError> {
    collect_assets_reserved(site, options, &BTreeSet::new(), Path::new("")).await
}

pub(crate) async fn collect_assets_reserved(
    site: &Site,
    options: CollectStaticOptions,
    reserved: &BTreeSet<PathBuf>,
    reserved_base: &Path,
) -> Result<CollectStaticReport, StaticAssetError> {
    if options.glob.is_some() && options.clean {
        return Err(StaticAssetError::InvalidOptions(
            "--clean cannot be used with --glob",
        ));
    }
    if options.clean && options.output.exists() {
        tokio::fs::remove_dir_all(&options.output)
            .await
            .map_err(|source| StaticAssetError::Io {
                path: options.output.clone(),
                source,
            })?;
    }
    tokio::fs::create_dir_all(&options.output)
        .await
        .map_err(|source| StaticAssetError::Io {
            path: options.output.clone(),
            source,
        })?;

    let dirs = site.asset_dirs();
    let set = DirSet::new(dirs);
    let mut seen = BTreeSet::new();
    let mut copied = 0;
    let matcher = match options.glob.as_deref() {
        Some(glob) => Some(
            GlobMatcher::new(glob)
                .map_err(|_| StaticAssetError::InvalidOptions("invalid glob pattern"))?,
        ),
        None => None,
    };

    for file in set.walk() {
        let rel = asset_relative_path(file.path());
        let Ok(rel) = rel else {
            continue;
        };
        let rel_match = rel.to_string_lossy().replace('\\', "/");
        if matcher
            .as_ref()
            .is_some_and(|matcher| !matcher.matches(&rel_match))
        {
            continue;
        }
        if !seen.insert(rel.clone()) {
            continue;
        }
        let dest =
            safe_join(&options.output, &rel).map_err(|()| StaticAssetError::OutputEscape {
                path: rel.to_string_lossy().to_string(),
            })?;
        let conflict_path = reserved_base.join("assets").join(&rel);
        if reserved.contains(&conflict_path) {
            return Err(StaticAssetError::PageConflict {
                path: conflict_path.clone(),
                page: conflict_path,
            });
        }
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| StaticAssetError::Io {
                    path: parent.to_path_buf(),
                    source,
                })?;
        }
        let bytes = file
            .read_bytes_async()
            .await
            .map_err(|source| StaticAssetError::Io {
                path: file.path().to_path_buf(),
                source,
            })?;
        tokio::fs::write(&dest, bytes)
            .await
            .map_err(|source| StaticAssetError::Io { path: dest, source })?;
        copied += 1;
    }

    Ok(CollectStaticReport {
        copied,
        output: options.output,
    })
}
