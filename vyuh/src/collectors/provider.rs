use std::sync::Arc;

use crate::{
    Error, Site, callables,
    callables::{Callable, DataBox, FromSite, IntoArgPart},
};

use super::{UrlInfo, UrlRoles, error::StaticExportError, path};

pub struct UrlInfoContext {
    site: Site,
}

impl UrlInfoContext {
    fn new(site: Site) -> Self {
        Self { site }
    }
}

impl callables::HasSite for UrlInfoContext {
    fn site(&self) -> &Site {
        &self.site
    }
}

impl callables::IntoOutput<Error> for Vec<UrlInfo> {
    fn into_output(self) -> Result<DataBox, Error> {
        Ok(DataBox::new(self))
    }
}

impl callables::IntoReturnPart for Vec<UrlInfo> {
    fn into_return_part() -> callables::ReturnPart {
        callables::ReturnPart::Unknown
    }
}

#[derive(Clone)]
pub(crate) struct UrlInfoProvider {
    handler: Callable<UrlInfoContext, Error>,
    prefixes: Arc<Vec<String>>,
}

impl UrlInfoProvider {
    pub(crate) fn new<H, Args>(handler: H) -> Self
    where
        H: callables::Specable<Args, Output = Result<Vec<UrlInfo>, Error>> + Send + Sync + 'static,
        Args: callables::FromContext<UrlInfoContext> + callables::IntoArgSpecs + Send + 'static,
    {
        Self {
            handler: Callable::new(handler),
            prefixes: Arc::new(Vec::new()),
        }
    }

    fn with_prefix(&self, prefix: &str) -> Self {
        let mut prefixes = self.prefixes.as_ref().clone();
        prefixes.push(prefix.to_string());
        Self {
            handler: self.handler.clone(),
            prefixes: Arc::new(prefixes),
        }
    }

    async fn collect(&self, site: Site) -> Result<Vec<UrlInfo>, StaticExportError> {
        let output = self
            .handler
            .call(UrlInfoContext::new(site))
            .await
            .map_err(StaticExportError::UrlProvider)?;
        let urls = output
            .downcast_arc::<Vec<UrlInfo>>()
            .ok_or_else(|| Error::invalid("URL info provider returned an unexpected type"))?;
        Ok(urls
            .iter()
            .cloned()
            .map(|mut info| {
                for prefix in self.prefixes.iter() {
                    info.path = path::apply_prefix(prefix, &info.path);
                }
                info
            })
            .collect())
    }
}

#[derive(Clone, Default)]
pub(crate) struct UrlInfoRegistry {
    providers: Vec<UrlInfoProvider>,
}

impl UrlInfoRegistry {
    pub(crate) fn new() -> Self {
        Self {
            providers: Vec::new(),
        }
    }

    pub(crate) fn register(&mut self, provider: UrlInfoProvider) {
        self.providers.push(provider);
    }

    pub(crate) fn merge(&mut self, mut other: Self) {
        self.providers.append(&mut other.providers);
    }

    pub(crate) fn with_prefix(&mut self, prefix: &str) {
        self.providers = self
            .providers
            .iter()
            .map(|provider| provider.with_prefix(prefix))
            .collect();
    }

    pub(crate) async fn collect(&self, site: Site) -> Result<Vec<UrlInfo>, StaticExportError> {
        let mut merged = std::collections::BTreeMap::<String, UrlRoles>::new();
        for provider in &self.providers {
            for info in provider.collect(site.clone()).await? {
                path::validate_url(&info.path)?;
                merged
                    .entry(info.path)
                    .and_modify(|roles| roles.insert(info.roles))
                    .or_insert(info.roles);
            }
        }
        Ok(merged
            .into_iter()
            .map(|(path, roles)| UrlInfo { path, roles })
            .collect())
    }
}

impl FromSite for UrlInfoRegistry {
    fn from_site(_site: &Site) -> Result<Self, callables::CallError> {
        Err(callables::CallError::ExtractionFailed(
            "UrlInfoRegistry cannot be extracted by handlers".into(),
        ))
    }
}

impl IntoArgPart for UrlInfoRegistry {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}

pub fn provider<H, Args>(handler: H) -> UrlInfoProvider
where
    H: callables::Specable<Args, Output = Result<Vec<UrlInfo>, Error>> + Send + Sync + 'static,
    Args: callables::FromContext<UrlInfoContext> + callables::IntoArgSpecs + Send + 'static,
{
    UrlInfoProvider::new::<H, Args>(handler)
}
