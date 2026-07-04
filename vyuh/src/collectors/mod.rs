mod assets;
mod error;
mod export;
mod glob;
mod path;
mod provider;

pub use assets::{CollectStaticOptions, CollectStaticReport, collect_assets};
pub use error::{StaticAssetError, StaticExportError};
pub use export::{
    CollectPagesReport, Collectors, RenderedResponse, StaticExportOptions, StaticExportReport,
    export_static,
};
pub(crate) use provider::{
    UrlInfoContext, UrlInfoProvider, UrlInfoRegistry, provider as url_info_provider,
};

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    pub struct UrlRoles: u32 {
        const STATIC = 0b0001;
        const SITEMAP = 0b0010;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UrlInfo {
    pub path: String,
    pub roles: UrlRoles,
}

impl UrlInfo {
    pub fn new(path: impl Into<String>, roles: UrlRoles) -> Self {
        Self {
            path: path.into(),
            roles,
        }
    }

    pub fn static_page(path: impl Into<String>) -> Self {
        Self::new(path, UrlRoles::STATIC)
    }

    pub fn sitemap(path: impl Into<String>) -> Self {
        Self::new(path, UrlRoles::SITEMAP)
    }

    pub fn with_sitemap(mut self) -> Self {
        self.roles.insert(UrlRoles::SITEMAP);
        self
    }

    pub fn with_static_export(mut self) -> Self {
        self.roles.insert(UrlRoles::STATIC);
        self
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use axum::http::StatusCode;
    use rust_silos::Silo;
    use tempfile::TempDir;

    use crate::{
        Error, Site, SiteConf,
        bundles::{self, RouteConf},
        callables,
        console::ConsoleConf,
        embed,
        routes::{Json, Methods},
    };

    use super::{
        CollectStaticOptions, StaticExportOptions, UrlInfo, UrlRoles, collect_assets, export_static,
    };

    async fn home() -> Json<&'static str> {
        Json("home")
    }

    async fn about() -> Json<&'static str> {
        Json("about")
    }

    async fn missing() -> StatusCode {
        StatusCode::NOT_FOUND
    }

    async fn direct_urls(_: Site) -> Result<Vec<UrlInfo>, Error> {
        Ok(vec![
            UrlInfo::static_page("/"),
            UrlInfo::new("/about", UrlRoles::STATIC | UrlRoles::SITEMAP),
            UrlInfo::sitemap("/sitemap.xml"),
        ])
    }

    #[crate::bundles::url_info]
    async fn macro_urls(_: Site) -> Result<Vec<UrlInfo>, Error> {
        Ok(vec![UrlInfo::static_page("/")])
    }

    async fn child_urls(_: Site) -> Result<Vec<UrlInfo>, Error> {
        Ok(vec![
            UrlInfo::static_page("/"),
            UrlInfo::sitemap("/feed.xml"),
        ])
    }

    async fn duplicate_static(_: Site) -> Result<Vec<UrlInfo>, Error> {
        Ok(vec![UrlInfo::static_page("/about")])
    }

    async fn invalid_urls(_: Site) -> Result<Vec<UrlInfo>, Error> {
        Ok(vec![UrlInfo::static_page("about")])
    }

    async fn site(bundle: bundles::Bundle) -> Site {
        Site::build(
            SiteConf::default()
                .log_init(false)
                .console(ConsoleConf::default().enabled(false)),
            bundle,
        )
        .await
        .unwrap()
    }

    fn route<H, T, Args>(handler: H, name: &'static str, path: &'static str) -> bundles::BundlePart
    where
        H: axum::handler::Handler<T, Site>
            + callables::Specable<Args>
            + Clone
            + Send
            + Sync
            + 'static,
        T: 'static,
        Args: callables::IntoArgSpecs + 'static,
    {
        bundles::route(
            handler,
            RouteConf {
                name: Cow::Borrowed(name),
                path: Cow::Borrowed(path),
                methods: Methods::GET,
                slash: None,
            },
        )
    }

    #[tokio::test]
    async fn direct_and_macro_url_info_are_collected() {
        let app = bundles::bundle([bundles::url_info(direct_urls)]).merge(bundles::bundle! {
            macro_urls,
        });
        let site = site(app).await;
        let urls = site.url_info().await.unwrap();

        assert_eq!(urls.len(), 3);
        assert!(
            urls.iter()
                .any(|info| info.path == "/" && info.roles.contains(UrlRoles::STATIC))
        );
        assert!(
            urls.iter()
                .any(|info| info.path == "/about" && info.roles.contains(UrlRoles::SITEMAP))
        );
    }

    #[tokio::test]
    async fn merge_and_prefix_apply_to_url_info() {
        let child = bundles::bundle([bundles::url_info(child_urls)]).with_prefix("/blog");
        let app = bundles::Bundle::new().merge(child).with_prefix("/site");
        let site = site(app).await;
        let urls = site.url_info().await.unwrap();

        assert!(urls.iter().any(|info| info.path == "/site/blog/"));
        assert!(urls.iter().any(|info| info.path == "/site/blog/feed.xml"));
    }

    #[tokio::test]
    async fn duplicate_final_url_merges_roles() {
        let app = bundles::bundle([
            bundles::url_info(direct_urls),
            bundles::url_info(duplicate_static),
        ]);
        let site = site(app).await;
        let urls = site.url_info().await.unwrap();
        let about = urls.iter().find(|info| info.path == "/about").unwrap();

        assert!(about.roles.contains(UrlRoles::STATIC));
        assert!(about.roles.contains(UrlRoles::SITEMAP));
    }

    #[tokio::test]
    async fn invalid_url_info_fails_collection() {
        let app = bundles::bundle([bundles::url_info(invalid_urls)]);
        let site = site(app).await;
        let err = site.url_info().await.unwrap_err();

        assert!(err.to_string().contains("must start"));
    }

    #[tokio::test]
    async fn static_export_filters_roles_and_maps_paths() {
        let out = TempDir::new().unwrap();
        let app = bundles::bundle([
            route(home, "home", "/"),
            route(about, "about", "/about"),
            bundles::url_info(direct_urls),
        ]);
        let site = site(app).await;
        let report = export_static(&site, StaticExportOptions::new(out.path()).clean(true))
            .await
            .unwrap();

        assert_eq!(report.pages, 2);
        assert!(out.path().join("index.html").exists());
        assert!(out.path().join("about.html").exists());
        assert!(!out.path().join("sitemap.xml").exists());
    }

    #[tokio::test]
    async fn collectors_glob_updates_only_matching_pages() {
        let out = TempDir::new().unwrap();
        let app = bundles::bundle([
            route(home, "home", "/"),
            route(about, "about", "/about"),
            bundles::url_info(direct_urls),
        ]);
        let site = site(app).await;
        let report = site
            .collectors()
            .output(out.path())
            .collect_pages(Some("/about".to_string()))
            .await
            .unwrap();

        assert_eq!(report.pages, 1);
        assert!(!out.path().join("index.html").exists());
        assert!(out.path().join("about.html").exists());
        assert!(!out.path().join("assets").exists());
    }

    #[tokio::test]
    async fn static_export_command_glob_updates_only_matching_pages() {
        let out = TempDir::new().unwrap();
        let app = bundles::bundle([
            route(home, "home", "/"),
            route(about, "about", "/about"),
            bundles::url_info(direct_urls),
        ]);
        let site = site(app).await;

        site.execute_command(
            "collect_pages",
            &["--output", out.path().to_str().unwrap(), "--glob", "/about"],
        )
        .await
        .unwrap();

        assert!(!out.path().join("index.html").exists());
        assert!(out.path().join("about.html").exists());
        assert!(!out.path().join("assets").exists());
    }

    #[tokio::test]
    async fn static_export_rejects_clean_with_glob() {
        let out = TempDir::new().unwrap();
        let app = bundles::bundle([route(home, "home", "/"), bundles::url_info(macro_urls)]);
        let site = site(app).await;
        let err = export_static(
            &site,
            StaticExportOptions::new(out.path())
                .clean(true)
                .glob(Some("/".to_string())),
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("--clean cannot be used with --glob")
        );
    }

    #[tokio::test]
    async fn static_export_fails_on_non_success() {
        async fn urls(_: Site) -> Result<Vec<UrlInfo>, Error> {
            Ok(vec![UrlInfo::static_page("/missing")])
        }
        let out = TempDir::new().unwrap();
        let app = bundles::bundle([
            bundles::route(
                missing,
                RouteConf {
                    name: Cow::Borrowed("missing"),
                    path: Cow::Borrowed("/missing"),
                    methods: Methods::GET,
                    slash: None,
                },
            ),
            bundles::url_info(urls),
        ]);
        let site = site(app).await;
        let err = export_static(&site, StaticExportOptions::new(out.path()))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("404"));
    }

    #[tokio::test]
    async fn collect_assets_copies_only_public_assets() {
        let root = TempDir::new().unwrap();
        let public = root.path().join("public/css");
        let private = root.path().join("templates");
        tokio::fs::create_dir_all(&public).await.unwrap();
        tokio::fs::create_dir_all(&private).await.unwrap();
        tokio::fs::write(public.join("app.css"), "body{}")
            .await
            .unwrap();
        tokio::fs::write(private.join("page.html"), "hidden")
            .await
            .unwrap();

        let out = TempDir::new().unwrap();
        let bundle = bundles::bundle([bundles::asset_dir(embed::Dir::new(Silo::new(
            root.path().to_str().unwrap(),
        )))]);
        let site = site(bundle).await;
        let report = collect_assets(&site, CollectStaticOptions::new(out.path()))
            .await
            .unwrap();

        assert_eq!(report.copied, 1);
        assert!(out.path().join("css/app.css").exists());
        assert!(!out.path().join("templates/page.html").exists());
    }

    #[tokio::test]
    async fn collect_assets_glob_copies_only_matching_assets() {
        let root = TempDir::new().unwrap();
        let css = root.path().join("public/css");
        let img = root.path().join("public/img");
        tokio::fs::create_dir_all(&css).await.unwrap();
        tokio::fs::create_dir_all(&img).await.unwrap();
        tokio::fs::write(css.join("app.css"), "body{}")
            .await
            .unwrap();
        tokio::fs::write(img.join("logo.svg"), "<svg />")
            .await
            .unwrap();

        let out = TempDir::new().unwrap();
        let bundle = bundles::bundle([bundles::asset_dir(embed::Dir::new(Silo::new(
            root.path().to_str().unwrap(),
        )))]);
        let site = site(bundle).await;
        let report = collect_assets(
            &site,
            CollectStaticOptions::new(out.path()).glob(Some("css/**".to_string())),
        )
        .await
        .unwrap();

        assert_eq!(report.copied, 1);
        assert!(out.path().join("css/app.css").exists());
        assert!(!out.path().join("img/logo.svg").exists());
    }

    #[tokio::test]
    async fn collectors_collect_assets_accepts_glob() {
        let root = TempDir::new().unwrap();
        let css = root.path().join("public/css");
        let js = root.path().join("public/js");
        tokio::fs::create_dir_all(&css).await.unwrap();
        tokio::fs::create_dir_all(&js).await.unwrap();
        tokio::fs::write(css.join("app.css"), "body{}")
            .await
            .unwrap();
        tokio::fs::write(js.join("app.js"), "console.log('ok')")
            .await
            .unwrap();

        let out = TempDir::new().unwrap();
        let bundle = bundles::bundle([bundles::asset_dir(embed::Dir::new(Silo::new(
            root.path().to_str().unwrap(),
        )))]);
        let site = site(bundle).await;
        let report = site
            .collectors()
            .output(out.path())
            .collect_assets(Some("js/**".to_string()))
            .await
            .unwrap();

        assert_eq!(report.copied, 1);
        assert!(!out.path().join("css/app.css").exists());
        assert!(out.path().join("js/app.js").exists());
    }

    #[tokio::test]
    async fn collect_assets_rejects_clean_with_glob() {
        let out = TempDir::new().unwrap();
        let site = site(bundles::Bundle::new()).await;
        let err = collect_assets(
            &site,
            CollectStaticOptions::new(out.path())
                .clean(true)
                .glob(Some("css/**".to_string())),
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("--clean cannot be used with --glob")
        );
    }

    #[tokio::test]
    async fn collect_assets_command_accepts_glob() {
        let root = TempDir::new().unwrap();
        let css = root.path().join("public/css");
        let img = root.path().join("public/img");
        tokio::fs::create_dir_all(&css).await.unwrap();
        tokio::fs::create_dir_all(&img).await.unwrap();
        tokio::fs::write(css.join("app.css"), "body{}")
            .await
            .unwrap();
        tokio::fs::write(img.join("logo.svg"), "<svg />")
            .await
            .unwrap();

        let out = TempDir::new().unwrap();
        let bundle = bundles::bundle([bundles::asset_dir(embed::Dir::new(Silo::new(
            root.path().to_str().unwrap(),
        )))]);
        let site = site(bundle).await;

        site.execute_command(
            "collect_assets",
            &["--output", out.path().to_str().unwrap(), "--glob", "img/**"],
        )
        .await
        .unwrap();

        assert!(!out.path().join("css/app.css").exists());
        assert!(out.path().join("img/logo.svg").exists());
    }

    #[tokio::test]
    async fn static_export_reuses_collect_assets_and_detects_conflicts() {
        async fn urls(_: Site) -> Result<Vec<UrlInfo>, Error> {
            Ok(vec![UrlInfo::static_page("/assets/css/app.css")])
        }
        async fn css_page() -> Json<&'static str> {
            Json("not css")
        }

        let root = TempDir::new().unwrap();
        let public = root.path().join("public/css");
        tokio::fs::create_dir_all(&public).await.unwrap();
        tokio::fs::write(public.join("app.css"), "body{}")
            .await
            .unwrap();

        let out = TempDir::new().unwrap();
        let app = bundles::bundle([
            bundles::asset_dir(embed::Dir::new(Silo::new(root.path().to_str().unwrap()))),
            bundles::route(
                css_page,
                RouteConf {
                    name: Cow::Borrowed("css_page"),
                    path: Cow::Borrowed("/assets/css/app.css"),
                    methods: Methods::GET,
                    slash: None,
                },
            ),
            bundles::url_info(urls),
        ]);
        let site = site(app).await;
        let err = export_static(&site, StaticExportOptions::new(out.path()))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("conflicts"));
    }

    #[tokio::test]
    async fn static_export_command_runs() {
        let out = TempDir::new().unwrap();
        let app = bundles::bundle([route(home, "home", "/"), bundles::url_info(macro_urls)]);
        let site = site(app).await;

        site.execute_command(
            "collect_pages",
            &["--output", out.path().to_str().unwrap(), "--clean"],
        )
        .await
        .unwrap();

        assert!(out.path().join("index.html").exists());
    }
}
