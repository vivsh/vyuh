use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use axum::http::StatusCode;
use bytes::Bytes;

use crate::Site;

use super::{
    CollectStaticOptions, StaticAssetError, StaticExportError, UrlInfo, UrlRoles,
    assets::{CollectStaticReport, collect_assets, collect_assets_reserved},
    glob::GlobMatcher,
    path::{url_to_output_path, url_to_relative_path},
};

#[derive(Debug, Clone)]
pub struct StaticExportOptions {
    pub output: PathBuf,
    pub clean: bool,
    pub glob: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StaticExportReport {
    pub pages: usize,
    pub assets: usize,
    pub output: PathBuf,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CollectPagesReport {
    pub pages: usize,
    pub output: PathBuf,
}

#[derive(Debug, Clone)]
pub struct RenderedResponse {
    pub status: StatusCode,
    pub content_type: Option<String>,
    pub body: Bytes,
}

impl StaticExportOptions {
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

#[derive(Clone)]
pub struct Collectors {
    site: Site,
    output: PathBuf,
    clean: bool,
}

impl Collectors {
    pub(crate) fn new(site: Site) -> Self {
        Self {
            site,
            output: PathBuf::from("dist"),
            clean: false,
        }
    }

    pub fn output(mut self, output: impl Into<PathBuf>) -> Self {
        self.output = output.into();
        self
    }

    pub fn clean(mut self, clean: bool) -> Self {
        self.clean = clean;
        self
    }

    pub async fn collect_pages(
        self,
        glob: Option<String>,
    ) -> Result<CollectPagesReport, StaticExportError> {
        export_pages(
            &self.site,
            PageExportOptions {
                output: self.output,
                clean: self.clean,
                glob,
            },
        )
        .await
    }

    pub async fn collect_assets(
        self,
        glob: Option<String>,
    ) -> Result<CollectStaticReport, StaticAssetError> {
        collect_assets(
            &self.site,
            CollectStaticOptions::new(self.output)
                .clean(self.clean)
                .glob(glob),
        )
        .await
    }
}

pub async fn export_static(
    site: &Site,
    options: StaticExportOptions,
) -> Result<StaticExportReport, StaticExportError> {
    if options.glob.is_some() && options.clean {
        return Err(StaticExportError::InvalidExportOptions(
            "--clean cannot be used with --glob",
        ));
    }
    let pages = site
        .collectors()
        .output(options.output.clone())
        .clean(options.clean)
        .collect_pages(options.glob.clone())
        .await?;
    if options.glob.is_some() {
        return Ok(StaticExportReport {
            pages: pages.pages,
            assets: 0,
            output: options.output,
        });
    }
    let urls = site.url_info().await?;
    let rendered_paths = rendered_page_paths(urls)?;
    let static_path = site.static_asset_path();
    let CollectStaticReport { copied, .. } = collect_assets_reserved(
        site,
        CollectStaticOptions::new(options.output.join(&static_path)),
        &rendered_paths,
        &static_path,
    )
    .await?;

    Ok(StaticExportReport {
        pages: pages.pages,
        assets: copied,
        output: options.output,
    })
}

struct PageExportOptions {
    output: PathBuf,
    clean: bool,
    glob: Option<String>,
}

async fn export_pages(
    site: &Site,
    options: PageExportOptions,
) -> Result<CollectPagesReport, StaticExportError> {
    if options.clean && options.output.exists() {
        tokio::fs::remove_dir_all(&options.output)
            .await
            .map_err(|source| StaticExportError::Io {
                path: options.output.clone(),
                source,
            })?;
    }
    tokio::fs::create_dir_all(&options.output)
        .await
        .map_err(|source| StaticExportError::Io {
            path: options.output.clone(),
            source,
        })?;

    let urls = site.url_info().await?;
    let mut output_paths = BTreeMap::<PathBuf, String>::new();
    let matcher = match options.glob.as_deref() {
        Some(glob) => Some(
            GlobMatcher::new(glob)
                .map_err(|_| StaticExportError::InvalidExportOptions("invalid glob pattern"))?,
        ),
        None => None,
    };
    let mut pages = 0;

    for info in urls
        .into_iter()
        .filter(|info| info.roles.contains(UrlRoles::STATIC))
        .filter(|info| {
            matcher
                .as_ref()
                .is_none_or(|matcher| matcher.matches(&info.path))
        })
    {
        let rel = url_to_relative_path(&info.path)?;
        if let Some(existing) = output_paths.insert(rel.clone(), info.path.clone()) {
            return Err(StaticExportError::DuplicateOutputPath {
                left: existing,
                right: info.path,
                path: rel,
            });
        }
        let out = url_to_output_path(&options.output, &info.path)?;
        let response = site.render_get(&info.path).await.map_err(|source| {
            StaticExportError::RenderFailed {
                url: info.path.clone(),
                source,
            }
        })?;
        if response.status != StatusCode::OK {
            return Err(StaticExportError::NonSuccess {
                url: info.path,
                status: response.status,
            });
        }
        if let Some(parent) = out.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| StaticExportError::Io {
                    path: parent.to_path_buf(),
                    source,
                })?;
        }
        tokio::fs::write(&out, response.body)
            .await
            .map_err(|source| StaticExportError::Io {
                path: out.clone(),
                source,
            })?;
        pages += 1;
    }

    Ok(CollectPagesReport {
        pages,
        output: options.output,
    })
}

fn rendered_page_paths(urls: Vec<UrlInfo>) -> Result<BTreeSet<PathBuf>, StaticExportError> {
    let mut output_paths = BTreeMap::<PathBuf, String>::new();
    for info in urls
        .into_iter()
        .filter(|info| info.roles.contains(UrlRoles::STATIC))
    {
        let rel = url_to_relative_path(&info.path)?;
        if let Some(existing) = output_paths.insert(rel.clone(), info.path.clone()) {
            return Err(StaticExportError::DuplicateOutputPath {
                left: existing,
                right: info.path,
                path: rel,
            });
        }
    }
    Ok(output_paths.into_keys().collect())
}
