use crate::{
    Site,
    bundles::Bundle,
    callables::{self, FromSite, IntoArgPart},
    embed,
};
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateConf {
    pub auto_escape: TemplateAutoEscape,
    pub undefined: TemplateUndefined,
    pub trim_blocks: bool,
    pub lstrip_blocks: bool,
    pub keep_trailing_newline: bool,
    pub date_formats: TemplateDateFormats,
}

impl Default for TemplateConf {
    fn default() -> Self {
        Self {
            auto_escape: TemplateAutoEscape::ByExtension,
            undefined: TemplateUndefined::Strict,
            trim_blocks: false,
            lstrip_blocks: false,
            keep_trailing_newline: true,
            date_formats: TemplateDateFormats::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemplateAutoEscape {
    ByExtension,
    Always,
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemplateUndefined {
    Strict,
    Lenient,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateDateFormats {
    pub date: String,
    pub time: String,
    pub datetime: String,
}

impl Default for TemplateDateFormats {
    fn default() -> Self {
        Self {
            date: "%Y-%m-%d".into(),
            time: "%H:%M".into(),
            datetime: "%Y-%m-%d %H:%M".into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TemplateFormatError {
    #[error("unsupported date/time value")]
    UnsupportedValue,

    #[error("invalid date/time value: {0}")]
    InvalidValue(String),
}

#[derive(Debug, thiserror::Error)]
pub enum TemplateError {
    #[error("Path error: {0}")]
    PathError(String),

    #[error("Template file error: {0}")]
    FileError(String),

    #[error("Template rendering error: {0}")]
    ParseError(String),

    #[error("Template not found: {0}")]
    NotFound(String),

    #[error("Render error: {0}")]
    RenderError(#[from] minijinja::Error),
}

pub struct TemplateEngine {
    env: minijinja::Environment<'static>,
}

impl TemplateEngine {
    pub fn new() -> Self {
        Self::from_conf(&TemplateConf::default(), chrono_tz::Tz::UTC)
    }

    pub(crate) fn from_conf(conf: &TemplateConf, timezone: chrono_tz::Tz) -> Self {
        let mut env = minijinja::Environment::new();
        register_builtin_helpers(&mut env, conf, timezone);
        TemplateEngine { env }
    }

    pub fn render<S: serde::Serialize>(
        &self,
        template_name: &str,
        context: &S,
    ) -> Result<String, TemplateError> {
        self.env
            .get_template(template_name)
            .map_err(|e| {
                TemplateError::NotFound(format!("Template '{}' not found: {}", template_name, e))
            })?
            .render(context)
            .map_err(|e| TemplateError::RenderError(e).into())
    }

    pub fn html<S: serde::Serialize>(
        &self,
        template_name: &str,
        context: &S,
    ) -> Result<axum::response::Html<String>, TemplateError> {
        Ok(axum::response::Html(self.render(template_name, context)?))
    }

    pub fn exists(&self, template_name: &str) -> bool {
        self.env.templates().any(|(name, _)| name == template_name)
    }

    pub fn names(&self) -> Vec<String> {
        let mut names = self
            .env
            .templates()
            .map(|(name, _)| name.to_string())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    pub(crate) fn inject_templates(&mut self, bundle: &Bundle) -> Result<(), TemplateError> {
        let mut dir_vec: Vec<embed::Dir> = Vec::new();

        for asset_dir in &bundle.asset_dirs {
            dir_vec.push(asset_dir.clone());
        }

        for file in embed::DirSet::new(dir_vec).walk_override() {
            self.inject_file(file, Some("templates/"))?;
        }

        Ok(())
    }

    fn inject_file(
        &mut self,
        file: embed::File,
        prefix: Option<&str>,
    ) -> Result<(), TemplateError> {
        let path = file.path();
        let name = if let Some(prefix) = prefix {
            if !path.starts_with(prefix) {
                return Ok(());
            }
            path.strip_prefix(prefix)
                .map(|s| s.to_string_lossy().to_string())
                .map_err(|_| {
                    TemplateError::PathError(format!(
                        "Failed to strip prefix from template path: {}",
                        path.display()
                    ))
                })?
        } else {
            path.to_string_lossy().to_string()
        };

        if name.is_empty() {
            return Ok(());
        }

        let content = file.read_bytes_sync().map_err(|e| {
            TemplateError::FileError(format!(
                "Failed to read template file: {}: {}",
                path.display(),
                e
            ))
        })?;
        let body = String::from_utf8(content).map_err(|e| {
            TemplateError::FileError(format!(
                "Invalid UTF-8 in template file: {}: {}",
                path.display(),
                e
            ))
        })?;
        self.env.add_template_owned(name, body)?;
        Ok(())
    }

    pub fn manager<'a>(&'a self) -> TemplateManager<'a> {
        TemplateManager { engine: self }
    }
}

fn register_builtin_helpers(
    env: &mut minijinja::Environment<'static>,
    conf: &TemplateConf,
    timezone: chrono_tz::Tz,
) {
    env.add_global(
        "STATIC_URL",
        minijinja::Value::from_safe_string(crate::assets::public_asset_url("")),
    );
    env.add_function("asset", |path: String| {
        minijinja::Value::from_safe_string(crate::assets::public_asset_url(&path))
    });
    env.add_function("now", || chrono::Utc::now().to_rfc3339());
    env.add_filter("linebreaksbr", linebreaksbr_filter);

    let date_format = conf.date_formats.date.clone();
    env.add_filter("date", move |value: minijinja::Value| {
        format_template_datetime_value(value, timezone, &date_format)
    });

    let date_format = conf.date_formats.date.clone();
    env.add_filter(
        "format_date",
        move |value: minijinja::Value, pattern: Option<String>| {
            format_template_datetime_value(
                value,
                timezone,
                pattern.as_deref().unwrap_or(&date_format),
            )
        },
    );

    let time_format = conf.date_formats.time.clone();
    env.add_filter("time", move |value: minijinja::Value| {
        format_template_datetime_value(value, timezone, &time_format)
    });

    let time_format = conf.date_formats.time.clone();
    env.add_filter(
        "format_time",
        move |value: minijinja::Value, pattern: Option<String>| {
            format_template_datetime_value(
                value,
                timezone,
                pattern.as_deref().unwrap_or(&time_format),
            )
        },
    );

    let datetime_format = conf.date_formats.datetime.clone();
    env.add_filter("datetime", move |value: minijinja::Value| {
        format_template_datetime_value(value, timezone, &datetime_format)
    });

    let datetime_format = conf.date_formats.datetime.clone();
    env.add_filter(
        "format_datetime",
        move |value: minijinja::Value, pattern: Option<String>| {
            format_template_datetime_value(
                value,
                timezone,
                pattern.as_deref().unwrap_or(&datetime_format),
            )
        },
    );

    let local_date_format = conf.date_formats.date.clone();
    env.add_function("localdate", move || {
        Ok::<_, minijinja::Error>(minijinja::Value::from_safe_string(
            chrono::Utc::now()
                .with_timezone(&timezone)
                .format(&local_date_format)
                .to_string(),
        ))
    });

    let local_datetime_format = conf.date_formats.datetime.clone();
    env.add_function("localdatetime", move || {
        Ok::<_, minijinja::Error>(minijinja::Value::from_safe_string(
            chrono::Utc::now()
                .with_timezone(&timezone)
                .format(&local_datetime_format)
                .to_string(),
        ))
    });
}

fn format_template_datetime_value(
    value: minijinja::Value,
    timezone: chrono_tz::Tz,
    pattern: &str,
) -> Result<minijinja::Value, minijinja::Error> {
    let value = value.as_str().ok_or_else(|| {
        minijinja::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            format!("expected RFC3339 date/time string, got {}", value.kind()),
        )
    })?;
    let datetime = chrono::DateTime::parse_from_rfc3339(value).map_err(|err| {
        minijinja::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            format!("invalid RFC3339 date/time string: {err}"),
        )
    })?;
    Ok(minijinja::Value::from_safe_string(
        datetime
            .with_timezone(&timezone)
            .format(pattern)
            .to_string(),
    ))
}

fn linebreaksbr_filter(
    state: &minijinja::State,
    value: minijinja::Value,
) -> Result<minijinja::Value, minijinja::Error> {
    let escaped = minijinja::filters::escape(state, &value)?;
    let text = escaped
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| escaped.to_string());
    let html = text
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .replace('\n', "<br>\n");
    Ok(minijinja::Value::from_safe_string(html))
}

pub trait IntoTemplateDateTime {
    fn into_template_datetime(
        self,
        site: &Site,
    ) -> Result<chrono::DateTime<chrono_tz::Tz>, TemplateFormatError>;
}

impl IntoTemplateDateTime for chrono::DateTime<chrono::Utc> {
    fn into_template_datetime(
        self,
        site: &Site,
    ) -> Result<chrono::DateTime<chrono_tz::Tz>, TemplateFormatError> {
        Ok(self.with_timezone(&site.timezone()))
    }
}

impl IntoTemplateDateTime for &str {
    fn into_template_datetime(
        self,
        site: &Site,
    ) -> Result<chrono::DateTime<chrono_tz::Tz>, TemplateFormatError> {
        chrono::DateTime::parse_from_rfc3339(self)
            .map(|dt| dt.with_timezone(&site.timezone()))
            .map_err(|err| TemplateFormatError::InvalidValue(err.to_string()))
    }
}

impl IntoTemplateDateTime for String {
    fn into_template_datetime(
        self,
        site: &Site,
    ) -> Result<chrono::DateTime<chrono_tz::Tz>, TemplateFormatError> {
        self.as_str().into_template_datetime(site)
    }
}

pub fn format_date<T>(
    site: &Site,
    value: T,
    pattern: Option<&str>,
) -> Result<String, TemplateFormatError>
where
    T: IntoTemplateDateTime,
{
    let pattern = pattern.unwrap_or(&site.conf().templates.date_formats.date);
    Ok(value
        .into_template_datetime(site)?
        .format(pattern)
        .to_string())
}

pub fn format_time<T>(
    site: &Site,
    value: T,
    pattern: Option<&str>,
) -> Result<String, TemplateFormatError>
where
    T: IntoTemplateDateTime,
{
    let pattern = pattern.unwrap_or(&site.conf().templates.date_formats.time);
    Ok(value
        .into_template_datetime(site)?
        .format(pattern)
        .to_string())
}

pub fn format_datetime<T>(
    site: &Site,
    value: T,
    pattern: Option<&str>,
) -> Result<String, TemplateFormatError>
where
    T: IntoTemplateDateTime,
{
    let pattern = pattern.unwrap_or(&site.conf().templates.date_formats.datetime);
    Ok(value
        .into_template_datetime(site)?
        .format(pattern)
        .to_string())
}

pub fn localdate<T>(site: &Site, value: Option<T>) -> Result<String, TemplateFormatError>
where
    T: IntoTemplateDateTime,
{
    match value {
        Some(value) => format_date(site, value, None),
        None => format_date(site, chrono::Utc::now(), None),
    }
}

pub fn localdatetime<T>(site: &Site, value: Option<T>) -> Result<String, TemplateFormatError>
where
    T: IntoTemplateDateTime,
{
    match value {
        Some(value) => format_datetime(site, value, None),
        None => format_datetime(site, chrono::Utc::now(), None),
    }
}

pub trait TemplateRender {
    fn render<S: serde::Serialize>(
        &self,
        template_name: &str,
        context: &S,
    ) -> Result<String, TemplateError>;

    fn html<S: serde::Serialize>(
        &self,
        template_name: &str,
        context: &S,
    ) -> Result<axum::response::Html<String>, TemplateError>;

    fn exists(&self, template_name: &str) -> bool;

    fn names(&self) -> Vec<String>;
}

impl TemplateRender for TemplateEngine {
    fn render<S: serde::Serialize>(
        &self,
        template_name: &str,
        context: &S,
    ) -> Result<String, TemplateError> {
        TemplateEngine::render(self, template_name, context)
    }

    fn html<S: serde::Serialize>(
        &self,
        template_name: &str,
        context: &S,
    ) -> Result<axum::response::Html<String>, TemplateError> {
        TemplateEngine::html(self, template_name, context)
    }

    fn exists(&self, template_name: &str) -> bool {
        TemplateEngine::exists(self, template_name)
    }

    fn names(&self) -> Vec<String> {
        TemplateEngine::names(self)
    }
}

pub struct TemplateManager<'a> {
    engine: &'a TemplateEngine,
}

impl<'a> TemplateManager<'a> {
    pub fn render<S: serde::Serialize>(
        &self,
        template_name: &str,
        context: &S,
    ) -> Result<String, TemplateError> {
        self.engine.render(template_name, context)
    }

    pub fn html<S: serde::Serialize>(
        &self,
        template_name: &str,
        context: &S,
    ) -> Result<axum::response::Html<String>, TemplateError> {
        self.engine.html(template_name, context)
    }

    pub fn exists(&self, template_name: &str) -> bool {
        self.engine.exists(template_name)
    }

    pub fn names(&self) -> Vec<String> {
        self.engine.names()
    }
}

impl<'a> TemplateRender for TemplateManager<'a> {
    fn render<S: serde::Serialize>(
        &self,
        template_name: &str,
        context: &S,
    ) -> Result<String, TemplateError> {
        TemplateManager::render(self, template_name, context)
    }

    fn html<S: serde::Serialize>(
        &self,
        template_name: &str,
        context: &S,
    ) -> Result<axum::response::Html<String>, TemplateError> {
        TemplateManager::html(self, template_name, context)
    }

    fn exists(&self, template_name: &str) -> bool {
        TemplateManager::exists(self, template_name)
    }

    fn names(&self) -> Vec<String> {
        TemplateManager::names(self)
    }
}

#[derive(Clone)]
pub struct Templates {
    site: Site,
}

impl Templates {
    pub(crate) fn new(site: Site) -> Self {
        Self { site }
    }

    pub fn render<S: serde::Serialize>(
        &self,
        template_name: &str,
        context: &S,
    ) -> Result<String, TemplateError> {
        self.site.template_engine().render(template_name, context)
    }

    pub fn html<S: serde::Serialize>(
        &self,
        template_name: &str,
        context: &S,
    ) -> Result<axum::response::Html<String>, TemplateError> {
        Ok(axum::response::Html(self.render(template_name, context)?))
    }

    pub fn exists(&self, template_name: &str) -> bool {
        self.site.template_engine().exists(template_name)
    }

    pub fn names(&self) -> Vec<String> {
        self.site.template_engine().names()
    }
}

impl TemplateRender for Templates {
    fn render<S: serde::Serialize>(
        &self,
        template_name: &str,
        context: &S,
    ) -> Result<String, TemplateError> {
        Templates::render(self, template_name, context)
    }

    fn html<S: serde::Serialize>(
        &self,
        template_name: &str,
        context: &S,
    ) -> Result<axum::response::Html<String>, TemplateError> {
        Templates::html(self, template_name, context)
    }

    fn exists(&self, template_name: &str) -> bool {
        Templates::exists(self, template_name)
    }

    fn names(&self) -> Vec<String> {
        Templates::names(self)
    }
}

impl FromSite for Templates {
    fn from_site(site: &Site) -> Result<Self, callables::CallError> {
        Ok(Templates::new(site.clone()))
    }
}

impl axum::extract::FromRequestParts<Site> for Templates {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        _parts: &mut axum::http::request::Parts,
        state: &Site,
    ) -> Result<Self, Self::Rejection> {
        Ok(Templates::new(state.clone()))
    }
}

impl IntoArgPart for Templates {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}

impl IntoResponse for TemplateError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            TemplateError::NotFound(_) => axum::http::StatusCode::NOT_FOUND,
            _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        };
        crate::errors::ErrorReport::new(
            status,
            crate::errors::ErrorSourceKind::Template,
            match status {
                axum::http::StatusCode::NOT_FOUND => "template_not_found",
                _ => "template_error",
            },
            self.to_string(),
        )
        .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{bundles, embed};

    fn write_file(path: &std::path::Path, body: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, body).unwrap();
    }

    #[test]
    fn bundle_templates_load_from_asset_dir() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path().join("templates/hello.html").as_path(),
            "Hello {{ name }}",
        );

        let bundle = bundles::bundle([bundles::asset_dir(embed::Dir::new(rust_silos::Silo::new(
            dir.path().to_str().unwrap(),
        )))]);
        let mut engine = TemplateEngine::new();
        engine.inject_templates(&bundle).unwrap();

        assert!(engine.exists("hello.html"));
        assert_eq!(
            engine
                .render("hello.html", &serde_json::json!({ "name": "Vyuh" }))
                .unwrap(),
            "Hello Vyuh"
        );
    }

    #[test]
    fn asset_templates_strip_templates_prefix() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path().join("templates/dashboard/base.html").as_path(),
            "Dashboard {{ section }}",
        );
        write_file(
            dir.path().join("public/dashboard/dashboard.css").as_path(),
            "body {}",
        );

        let bundle = bundles::bundle([bundles::asset_dir(embed::Dir::new(rust_silos::Silo::new(
            dir.path().to_str().unwrap(),
        )))]);
        let mut engine = TemplateEngine::new();
        engine.inject_templates(&bundle).unwrap();

        assert!(engine.exists("dashboard/base.html"));
        assert!(!engine.exists("public/dashboard/dashboard.css"));
        assert_eq!(
            engine
                .render(
                    "dashboard/base.html",
                    &serde_json::json!({ "section": "operations" }),
                )
                .unwrap(),
            "Dashboard operations"
        );
    }

    #[test]
    fn builtin_asset_helpers_use_public_asset_mount() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path().join("templates/page.html").as_path(),
            r#"{{ STATIC_URL }}|{{ asset("css/app.css") }}|{{ asset("/img/logo.png") }}"#,
        );
        let bundle = bundles::bundle([bundles::asset_dir(embed::Dir::new(rust_silos::Silo::new(
            dir.path().to_str().unwrap(),
        )))]);
        let mut engine = TemplateEngine::new();
        engine.inject_templates(&bundle).unwrap();

        assert_eq!(
            engine.render("page.html", &serde_json::json!({})).unwrap(),
            "/assets/|/assets/css/app.css|/assets/img/logo.png"
        );
    }

    #[test]
    fn builtin_datetime_filters_use_template_conf_and_timezone() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path().join("templates/page.html").as_path(),
            "{{ value|date }}|{{ value|time }}|{{ value|datetime }}|{{ value|format_datetime(\"%d %b %Y\") }}",
        );
        let bundle = bundles::bundle([bundles::asset_dir(embed::Dir::new(rust_silos::Silo::new(
            dir.path().to_str().unwrap(),
        )))]);
        let mut conf = TemplateConf::default();
        conf.date_formats.date = "%Y/%m/%d".to_string();
        conf.date_formats.time = "%H:%M".to_string();
        conf.date_formats.datetime = "%Y/%m/%d %H:%M".to_string();
        let mut engine = TemplateEngine::from_conf(&conf, chrono_tz::Asia::Kolkata);
        engine.inject_templates(&bundle).unwrap();

        assert_eq!(
            engine
                .render(
                    "page.html",
                    &serde_json::json!({ "value": "2026-06-30T00:30:00Z" }),
                )
                .unwrap(),
            "2026/06/30|06:00|2026/06/30 06:00|30 Jun 2026"
        );
    }

    #[test]
    fn builtin_linebreaksbr_filter_escapes_text_and_preserves_breaks() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path().join("templates/page.html").as_path(),
            "{{ body|linebreaksbr }}",
        );
        let bundle = bundles::bundle([bundles::asset_dir(embed::Dir::new(rust_silos::Silo::new(
            dir.path().to_str().unwrap(),
        )))]);
        let mut engine = TemplateEngine::new();
        engine.inject_templates(&bundle).unwrap();

        assert_eq!(
            engine
                .render(
                    "page.html",
                    &serde_json::json!({ "body": "<strong>Hello</strong>\nWorld" }),
                )
                .unwrap(),
            "&lt;strong&gt;Hello&lt;&#x2f;strong&gt;<br>\nWorld"
        );
    }

    /// Verifies that a later asset directory overrides a matching template path.
    #[test]
    fn later_assets_override_templates() {
        let first = tempfile::tempdir().unwrap();
        write_file(
            first.path().join("templates/shared.html").as_path(),
            "first",
        );
        let second = tempfile::tempdir().unwrap();
        write_file(
            second.path().join("templates/shared.html").as_path(),
            "second",
        );
        let bundle = bundles::bundle([
            bundles::asset_dir(embed::Dir::new(rust_silos::Silo::new(
                first.path().to_str().unwrap(),
            ))),
            bundles::asset_dir(embed::Dir::new(rust_silos::Silo::new(
                second.path().to_str().unwrap(),
            ))),
        ]);

        let mut engine = TemplateEngine::new();
        engine.inject_templates(&bundle).unwrap();

        assert_eq!(
            engine
                .render("shared.html", &serde_json::json!({}))
                .unwrap(),
            "second"
        );
    }

    #[test]
    fn invalid_template_syntax_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path().join("templates/broken.html").as_path(),
            "{% if %}",
        );
        let bundle = bundles::bundle([bundles::asset_dir(embed::Dir::new(rust_silos::Silo::new(
            dir.path().to_str().unwrap(),
        )))]);

        let mut engine = TemplateEngine::new();
        let err = engine.inject_templates(&bundle).unwrap_err();

        assert!(matches!(err, TemplateError::RenderError(_)));
    }
}
