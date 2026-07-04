use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use bytes::Bytes;
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use tempfile::TempPath;
use thiserror::Error;
use tokio::io::AsyncWriteExt;

use crate::callables::{MultipartApiField, MultipartApiFieldKind, MultipartApiSpec};
use crate::errors::{ErrorReport, ErrorSourceKind};
use crate::file_storage::UploadConf;
use crate::validation::{Path as ValidationPath, ValidationError, ValidationReport};
use crate::{Site, validation::Valid};

#[derive(Debug, Clone)]
pub struct MultipartForm<T>(pub T);

impl<T> MultipartForm<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> Deref for MultipartForm<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> AsRef<T> for MultipartForm<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

pub trait MultipartData: Sized + Send + 'static {
    fn multipart_spec() -> MultipartSpec;
    fn from_multipart(map: MultipartMap) -> Result<Self, MultipartError>;
}

#[derive(Debug, Clone)]
pub struct UploadedFile(Arc<UploadedFileInner>);

#[derive(Debug)]
struct UploadedFileInner {
    field_name: String,
    file_name: Option<String>,
    content_type: Option<String>,
    sniffed_content_type: Option<String>,
    size: u64,
    data: UploadedFileData,
}

#[derive(Debug)]
enum UploadedFileData {
    Memory(Bytes),
    Temp { path: TempPath },
}

impl UploadedFile {
    pub fn field_name(&self) -> &str {
        &self.0.field_name
    }

    pub fn file_name(&self) -> Option<&str> {
        self.0.file_name.as_deref()
    }

    pub fn content_type(&self) -> Option<&str> {
        self.0.content_type.as_deref()
    }

    pub fn sniffed_content_type(&self) -> Option<&str> {
        self.0.sniffed_content_type.as_deref()
    }

    pub fn size(&self) -> u64 {
        self.0.size
    }

    pub fn temp_path(&self) -> Option<&Path> {
        match &self.0.data {
            UploadedFileData::Temp { path } => Some(path.as_ref()),
            UploadedFileData::Memory(_) => None,
        }
    }

    pub fn is_memory(&self) -> bool {
        matches!(self.0.data, UploadedFileData::Memory(_))
    }

    pub fn memory_bytes(&self) -> Option<&[u8]> {
        match &self.0.data {
            UploadedFileData::Memory(bytes) => Some(bytes.as_ref()),
            UploadedFileData::Temp { .. } => None,
        }
    }

    pub async fn open(&self) -> Result<tokio::fs::File, MultipartError> {
        match &self.0.data {
            UploadedFileData::Temp { path } => Ok(tokio::fs::File::open(path).await?),
            UploadedFileData::Memory(bytes) => {
                let mut temp = tempfile::Builder::new()
                    .prefix("vyuh-upload-open-")
                    .tempfile()
                    .map_err(MultipartError::from)?;
                std::io::Write::write_all(&mut temp, bytes)?;
                let reopened = temp.reopen()?;
                Ok(tokio::fs::File::from_std(reopened))
            }
        }
    }

    pub async fn bytes_limited(&self, max: u64) -> Result<Bytes, MultipartError> {
        if self.size() > max {
            return Err(MultipartError::too_large(
                self.field_name(),
                format!("file exceeds {max} bytes"),
            ));
        }
        match &self.0.data {
            UploadedFileData::Memory(bytes) => Ok(bytes.clone()),
            UploadedFileData::Temp { path } => Ok(tokio::fs::read(path).await?.into()),
        }
    }
}

impl JsonSchema for UploadedFile {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("UploadedFile")
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "format": "binary"
        })
    }
}

#[derive(Debug, Clone)]
pub struct UploadedText {
    value: String,
}

impl UploadedText {
    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub fn into_inner(self) -> String {
        self.value
    }
}

#[derive(Debug, Clone)]
pub struct JsonPart<T>(pub T);

impl<T> JsonPart<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

#[derive(Debug, Clone, Default)]
pub struct MultipartMap {
    texts: BTreeMap<String, Vec<String>>,
    files: BTreeMap<String, Vec<UploadedFile>>,
}

impl MultipartMap {
    pub fn validate(self, spec: MultipartSpec) -> Result<Self, MultipartError> {
        spec.validate_map(&self)?;
        Ok(self)
    }

    pub fn text(&self, name: &str) -> Result<&str, MultipartError> {
        self.texts
            .get(name)
            .and_then(|values| values.first())
            .map(String::as_str)
            .ok_or_else(|| MultipartError::missing_field(name))
    }

    pub fn text_opt(&self, name: &str) -> Option<&str> {
        self.texts
            .get(name)
            .and_then(|values| values.first())
            .map(String::as_str)
    }

    pub fn text_vec(&self, name: &str) -> Vec<&str> {
        self.texts
            .get(name)
            .map(|values| values.iter().map(String::as_str).collect())
            .unwrap_or_default()
    }

    pub fn json<T: serde::de::DeserializeOwned>(
        &self,
        name: &str,
    ) -> Result<JsonPart<T>, MultipartError> {
        let raw = self.text(name)?;
        serde_json::from_str(raw)
            .map(JsonPart)
            .map_err(|err| MultipartError::invalid_field(name, err.to_string()))
    }

    pub fn file(&self, name: &str) -> Result<&UploadedFile, MultipartError> {
        self.files
            .get(name)
            .and_then(|values| values.first())
            .ok_or_else(|| MultipartError::missing_field(name))
    }

    pub fn file_opt(&self, name: &str) -> Option<&UploadedFile> {
        self.files.get(name).and_then(|values| values.first())
    }

    pub fn files(&self, name: &str) -> &[UploadedFile] {
        self.files.get(name).map(Vec::as_slice).unwrap_or(&[])
    }
}

#[derive(Debug, Clone, Default)]
pub struct MultipartSpec {
    text: BTreeMap<String, FieldRule>,
    files: BTreeMap<String, FileRule>,
    allow_unknown: bool,
}

impl MultipartSpec {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn text(mut self, name: impl Into<String>, rule: FieldRule) -> Self {
        self.text.insert(name.into(), rule);
        self
    }

    pub fn file(mut self, name: impl Into<String>, rule: FileRule) -> Self {
        self.files.insert(name.into(), rule);
        self
    }

    pub fn allow_unknown(mut self, allow: bool) -> Self {
        self.allow_unknown = allow;
        self
    }

    /// Returns declared text field rules keyed by multipart field name.
    pub fn text_fields(&self) -> &BTreeMap<String, FieldRule> {
        &self.text
    }

    /// Returns declared file field rules keyed by multipart field name.
    pub fn file_fields(&self) -> &BTreeMap<String, FileRule> {
        &self.files
    }

    /// Returns whether undeclared multipart fields are accepted.
    pub fn allows_unknown(&self) -> bool {
        self.allow_unknown
    }

    fn rule_for(&self, name: &str, is_file: bool) -> Option<RuleRef<'_>> {
        if is_file {
            self.files.get(name).map(RuleRef::File)
        } else {
            self.text.get(name).map(RuleRef::Text)
        }
    }

    fn validate_map(&self, map: &MultipartMap) -> Result<(), MultipartError> {
        let mut report = ValidationReport::empty();
        for (name, rule) in &self.text {
            let values = map.texts.get(name).map(Vec::as_slice).unwrap_or(&[]);
            rule.validate_values(name, values, &mut report);
        }
        for (name, rule) in &self.files {
            let values = map.files.get(name).map(Vec::as_slice).unwrap_or(&[]);
            rule.validate_files(name, values, &mut report)?;
        }
        if !self.allow_unknown {
            for name in map.texts.keys() {
                if !self.text.contains_key(name) {
                    report.push(
                        ValidationPath::root().at_field(name.clone()),
                        ValidationError::new("unknown_field", "Unknown multipart field."),
                    );
                }
            }
            for name in map.files.keys() {
                if !self.files.contains_key(name) {
                    report.push(
                        ValidationPath::root().at_field(name.clone()),
                        ValidationError::new("unknown_field", "Unknown multipart field."),
                    );
                }
            }
        }
        if report.is_empty() {
            Ok(())
        } else {
            Err(MultipartError::Validation(report))
        }
    }
}

impl From<&MultipartSpec> for MultipartApiSpec {
    fn from(spec: &MultipartSpec) -> Self {
        let text = spec
            .text_fields()
            .iter()
            .map(|(name, rule)| MultipartApiField {
                name: name.clone(),
                kind: MultipartApiFieldKind::Text,
                required: rule.is_required(),
                multiple: rule.is_multiple(),
                max_length: rule.max_length_value(),
                max_bytes: rule.max_bytes_value(),
                content_types: Vec::new(),
                extensions: Vec::new(),
                sniff: None,
            });
        let files = spec
            .file_fields()
            .iter()
            .map(|(name, rule)| MultipartApiField {
                name: name.clone(),
                kind: MultipartApiFieldKind::File,
                required: rule.is_required(),
                multiple: rule.is_multiple(),
                max_length: None,
                max_bytes: rule.max_size_value(),
                content_types: rule.allowed_content_types(),
                extensions: rule.allowed_extensions(),
                sniff: rule.sniff_rule().map(ToOwned::to_owned),
            });
        Self {
            fields: text.chain(files).collect(),
            allow_unknown: spec.allows_unknown(),
        }
    }
}

enum RuleRef<'a> {
    Text(&'a FieldRule),
    File(&'a FileRule),
}

#[derive(Debug, Clone, Default)]
pub struct FieldRule {
    required: bool,
    max_length: Option<usize>,
    max_bytes: Option<u64>,
    multiple: bool,
}

impl FieldRule {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub fn max_length(mut self, max: usize) -> Self {
        self.max_length = Some(max);
        self
    }

    pub fn max_bytes(mut self, max: u64) -> Self {
        self.max_bytes = Some(max);
        self
    }

    pub fn multiple(mut self) -> Self {
        self.multiple = true;
        self
    }

    /// Returns whether the text field is required.
    pub fn is_required(&self) -> bool {
        self.required
    }

    /// Returns the maximum text length in Unicode scalar values.
    pub fn max_length_value(&self) -> Option<usize> {
        self.max_length
    }

    /// Returns the maximum accepted text size in bytes.
    pub fn max_bytes_value(&self) -> Option<u64> {
        self.max_bytes
    }

    /// Returns whether repeated text values are accepted.
    pub fn is_multiple(&self) -> bool {
        self.multiple
    }

    fn validate_values(&self, name: &str, values: &[String], report: &mut ValidationReport) {
        if self.required && values.is_empty() {
            report.push(
                ValidationPath::root().at_field(name.to_string()),
                ValidationError::new("required", "This field is required."),
            );
        }
        if !self.multiple && values.len() > 1 {
            report.push(
                ValidationPath::root().at_field(name.to_string()),
                ValidationError::new("duplicate_field", "This field may be supplied only once."),
            );
        }
        for value in values {
            if let Some(max) = self.max_length {
                if value.chars().count() > max {
                    report.push(
                        ValidationPath::root().at_field(name.to_string()),
                        ValidationError::new(
                            "max_length",
                            format!("Ensure this field has at most {max} characters."),
                        )
                        .with_param("max", max),
                    );
                }
            }
            if let Some(max) = self.max_bytes {
                if value.len() as u64 > max {
                    report.push(
                        ValidationPath::root().at_field(name.to_string()),
                        ValidationError::new(
                            "max_bytes",
                            format!("Ensure this field has at most {max} bytes."),
                        )
                        .with_param("max", max),
                    );
                }
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct FileRule {
    required: bool,
    max_size: Option<u64>,
    content_types: BTreeSet<String>,
    extensions: BTreeSet<String>,
    sniff: Option<SniffRule>,
    multiple: bool,
}

impl FileRule {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub fn max_size(mut self, max: u64) -> Self {
        self.max_size = Some(max);
        self
    }

    pub fn content_types<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.content_types = values
            .into_iter()
            .map(|value| value.into().to_ascii_lowercase())
            .collect();
        self
    }

    pub fn extensions<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.extensions = values
            .into_iter()
            .map(|value| value.into().trim_start_matches('.').to_ascii_lowercase())
            .collect();
        self
    }

    pub fn sniff(mut self, rule: SniffRule) -> Self {
        self.sniff = Some(rule);
        self
    }

    pub fn sniff_image(self) -> Self {
        self.sniff(SniffRule::image())
    }

    pub fn multiple(mut self) -> Self {
        self.multiple = true;
        self
    }

    /// Returns whether the file field is required.
    pub fn is_required(&self) -> bool {
        self.required
    }

    /// Returns the maximum accepted file size in bytes.
    pub fn max_size_value(&self) -> Option<u64> {
        self.max_size
    }

    /// Returns allowed declared file content types.
    pub fn allowed_content_types(&self) -> Vec<String> {
        self.content_types.iter().cloned().collect()
    }

    /// Returns allowed file name extensions without leading dots.
    pub fn allowed_extensions(&self) -> Vec<String> {
        self.extensions.iter().cloned().collect()
    }

    /// Returns the configured sniffing rule name, if any.
    pub fn sniff_rule(&self) -> Option<&'static str> {
        self.sniff.as_ref().map(SniffRule::as_api_name)
    }

    /// Returns whether repeated files are accepted.
    pub fn is_multiple(&self) -> bool {
        self.multiple
    }

    fn max_size_or(&self, fallback: u64) -> u64 {
        self.max_size.unwrap_or(fallback)
    }

    fn validate_headers(
        &self,
        field_name: &str,
        file_name: Option<&str>,
        content_type: Option<&str>,
    ) -> Result<(), MultipartError> {
        if !self.content_types.is_empty() {
            let Some(content_type) = content_type.map(str::to_ascii_lowercase) else {
                return Err(MultipartError::unsupported(
                    field_name,
                    "missing file content type",
                ));
            };
            if !self.content_types.contains(&content_type) {
                return Err(MultipartError::unsupported(
                    field_name,
                    format!("unsupported content type '{content_type}'"),
                ));
            }
        }
        if !self.extensions.is_empty() {
            let extension = file_name.and_then(|name| {
                name.rsplit_once('.')
                    .map(|(_, ext)| ext.to_ascii_lowercase())
            });
            match extension {
                Some(ext) if self.extensions.contains(&ext) => {}
                _ => {
                    return Err(MultipartError::unsupported(
                        field_name,
                        "unsupported file extension",
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_sniff(
        &self,
        field_name: &str,
        sniffed: Option<&str>,
    ) -> Result<(), MultipartError> {
        if let Some(rule) = &self.sniff {
            rule.validate(field_name, sniffed)?;
        }
        Ok(())
    }

    fn validate_files(
        &self,
        name: &str,
        values: &[UploadedFile],
        report: &mut ValidationReport,
    ) -> Result<(), MultipartError> {
        if self.required && values.is_empty() {
            report.push(
                ValidationPath::root().at_field(name.to_string()),
                ValidationError::new("required", "This field is required."),
            );
        }
        if !self.multiple && values.len() > 1 {
            report.push(
                ValidationPath::root().at_field(name.to_string()),
                ValidationError::new("duplicate_field", "This field may be supplied only once."),
            );
        }
        for file in values {
            if let Some(max_size) = self.max_size {
                if file.size() > max_size {
                    return Err(MultipartError::too_large(
                        name,
                        format!("file exceeds {max_size} bytes"),
                    ));
                }
            }
            self.validate_headers(name, file.file_name(), file.content_type())?;
            if self.sniff.is_some() {
                let detected = file
                    .sniffed_content_type()
                    .map(ToOwned::to_owned)
                    .or_else(|| detect_file_mime(file).ok().flatten());
                self.validate_sniff(name, detected.as_deref())?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum SniffRule {
    Image,
    Mime(BTreeSet<String>),
}

impl SniffRule {
    pub fn image() -> Self {
        Self::Image
    }

    pub fn mime<I, S>(values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Mime(
            values
                .into_iter()
                .map(|value| value.into().to_ascii_lowercase())
                .collect(),
        )
    }

    /// Returns a stable name for OpenAPI vendor metadata.
    pub fn as_api_name(&self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Mime(_) => "mime",
        }
    }

    fn validate(&self, field_name: &str, sniffed: Option<&str>) -> Result<(), MultipartError> {
        let Some(sniffed) = sniffed else {
            return Err(MultipartError::unsupported(
                field_name,
                "could not detect uploaded file type",
            ));
        };
        match self {
            SniffRule::Image if sniffed.starts_with("image/") => Ok(()),
            SniffRule::Mime(allowed) if allowed.contains(sniffed) => Ok(()),
            _ => Err(MultipartError::unsupported(
                field_name,
                format!("uploaded file content was detected as '{sniffed}'"),
            )),
        }
    }
}

#[derive(Debug, Error)]
pub enum MultipartError {
    #[error("invalid multipart form: {0}")]
    Invalid(String),

    #[error("unsupported multipart field '{field}': {reason}")]
    Unsupported { field: String, reason: String },

    #[error("multipart field '{field}' is too large: {reason}")]
    TooLarge { field: String, reason: String },

    #[error("validation failed")]
    Validation(ValidationReport),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl MultipartError {
    pub fn missing_field(field: &str) -> Self {
        let mut report = ValidationReport::empty();
        report.push(
            ValidationPath::root().at_field(field.to_string()),
            ValidationError::new("required", "This field is required."),
        );
        Self::Validation(report)
    }

    pub fn invalid_field(field: &str, message: impl Into<String>) -> Self {
        let mut report = ValidationReport::empty();
        report.push(
            ValidationPath::root().at_field(field.to_string()),
            ValidationError::new("invalid", message.into()),
        );
        Self::Validation(report)
    }

    pub fn unsupported(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Unsupported {
            field: field.into(),
            reason: reason.into(),
        }
    }

    pub fn too_large(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::TooLarge {
            field: field.into(),
            reason: reason.into(),
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            MultipartError::Invalid(_) | MultipartError::Io(_) => StatusCode::BAD_REQUEST,
            MultipartError::Unsupported { .. } => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            MultipartError::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            MultipartError::Validation(_) => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            MultipartError::Invalid(_) | MultipartError::Io(_) => "invalid_multipart_form",
            MultipartError::Unsupported { .. } => "unsupported_upload",
            MultipartError::TooLarge { .. } => "upload_too_large",
            MultipartError::Validation(_) => "validation_error",
        }
    }
}

impl From<MultipartError> for ErrorReport {
    fn from(value: MultipartError) -> Self {
        match value {
            MultipartError::Validation(report) => ErrorReport::validation(report),
            other => ErrorReport::new(
                other.status(),
                ErrorSourceKind::Parse,
                other.code(),
                other.to_string(),
            ),
        }
    }
}

impl From<MultipartError> for crate::Error {
    fn from(value: MultipartError) -> Self {
        match value {
            MultipartError::Validation(report) => crate::Error::from(report),
            other => crate::Error::bad_request(other.to_string()),
        }
    }
}

impl From<axum_extra::extract::multipart::MultipartError> for MultipartError {
    fn from(value: axum_extra::extract::multipart::MultipartError) -> Self {
        match value.status() {
            StatusCode::PAYLOAD_TOO_LARGE => Self::too_large("body", value.body_text()),
            _ => Self::Invalid(value.body_text()),
        }
    }
}

impl From<axum_extra::extract::multipart::MultipartRejection> for MultipartError {
    fn from(value: axum_extra::extract::multipart::MultipartRejection) -> Self {
        Self::Invalid(value.to_string())
    }
}

impl<T> FromRequest<Site> for MultipartForm<T>
where
    T: MultipartData,
{
    type Rejection = ErrorReport;

    async fn from_request(req: Request, state: &Site) -> Result<Self, Self::Rejection> {
        let spec = T::multipart_spec();
        let map = parse_multipart(req, state, Some(&spec)).await?;
        let value = T::from_multipart(map)?;
        Ok(Self(value))
    }
}

impl FromRequest<Site> for MultipartMap {
    type Rejection = ErrorReport;

    async fn from_request(req: Request, state: &Site) -> Result<Self, Self::Rejection> {
        parse_multipart(req, state, None).await.map_err(Into::into)
    }
}

async fn parse_multipart(
    req: Request,
    state: &Site,
    spec: Option<&MultipartSpec>,
) -> Result<MultipartMap, MultipartError> {
    let conf = state.conf().uploads.clone();
    let mut multipart = axum_extra::extract::Multipart::from_request(req, state)
        .await
        .map_err(MultipartError::from)?;
    let mut map = MultipartMap::default();
    let mut file_count = 0usize;
    let mut field_count = 0usize;
    let mut request_bytes = 0u64;

    while let Some(field) = multipart.next_field().await.map_err(MultipartError::from)? {
        field_count += 1;
        if field_count > conf.max_fields {
            return Err(MultipartError::too_large(
                "body",
                format!("multipart form exceeds {} fields", conf.max_fields),
            ));
        }
        let name = field
            .name()
            .ok_or_else(|| MultipartError::Invalid("multipart field is missing a name".into()))?
            .to_string();
        let is_file = field.file_name().is_some();
        match spec.and_then(|spec| spec.rule_for(&name, is_file)) {
            Some(RuleRef::Text(rule)) => {
                let text = read_text_field(field, &name, rule, &conf, &mut request_bytes).await?;
                map.texts.entry(name).or_default().push(text);
            }
            Some(RuleRef::File(rule)) => {
                if is_empty_optional_file(field.file_name(), rule) {
                    continue;
                }
                file_count += 1;
                if file_count > conf.max_files {
                    return Err(MultipartError::too_large(
                        "body",
                        format!("multipart form exceeds {} files", conf.max_files),
                    ));
                }
                let file = read_file_field(
                    field,
                    &name,
                    rule,
                    &conf,
                    state.project_dir(),
                    &mut request_bytes,
                )
                .await?;
                map.files.entry(name).or_default().push(file);
            }
            None if spec.is_some() => {
                return Err(MultipartError::invalid_field(
                    &name,
                    "Unknown multipart field.",
                ));
            }
            None if is_file => {
                file_count += 1;
                if file_count > conf.max_files {
                    return Err(MultipartError::too_large(
                        "body",
                        format!("multipart form exceeds {} files", conf.max_files),
                    ));
                }
                let default_rule = FileRule::new();
                let file = read_file_field(
                    field,
                    &name,
                    &default_rule,
                    &conf,
                    state.project_dir(),
                    &mut request_bytes,
                )
                .await?;
                map.files.entry(name).or_default().push(file);
            }
            None => {
                let default_rule = FieldRule::new();
                let text =
                    read_text_field(field, &name, &default_rule, &conf, &mut request_bytes).await?;
                map.texts.entry(name).or_default().push(text);
            }
        }
    }

    if let Some(spec) = spec {
        spec.validate_map(&map)?;
    }
    Ok(map)
}

fn is_empty_optional_file(file_name: Option<&str>, rule: &FileRule) -> bool {
    !rule.required && file_name.is_some_and(str::is_empty)
}

async fn read_text_field(
    mut field: axum_extra::extract::multipart::Field,
    name: &str,
    rule: &FieldRule,
    conf: &UploadConf,
    request_bytes: &mut u64,
) -> Result<String, MultipartError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = field.chunk().await.map_err(MultipartError::from)? {
        add_bytes(request_bytes, chunk.len() as u64, conf.max_request_bytes)?;
        if let Some(max) = rule.max_bytes {
            if bytes.len() as u64 + chunk.len() as u64 > max {
                return Err(MultipartError::too_large(
                    name,
                    format!("field exceeds {max} bytes"),
                ));
            }
        }
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes).map_err(|err| MultipartError::invalid_field(name, err.to_string()))
}

async fn read_file_field(
    mut field: axum_extra::extract::multipart::Field,
    name: &str,
    rule: &FileRule,
    conf: &UploadConf,
    project_dir: &Path,
    request_bytes: &mut u64,
) -> Result<UploadedFile, MultipartError> {
    let file_name = field.file_name().map(ToOwned::to_owned);
    let content_type = field.content_type().map(ToOwned::to_owned);
    rule.validate_headers(name, file_name.as_deref(), content_type.as_deref())?;

    let max_size = rule.max_size_or(conf.max_file_bytes);
    let temp_dir = conf
        .temp_dir
        .as_ref()
        .map(|dir| state_relative_temp_dir(project_dir, dir))
        .transpose()?;
    let mut size = 0u64;
    let mut memory = Vec::new();
    let mut temp: Option<(TempPath, tokio::fs::File)> = None;
    let mut sniff_prefix = Vec::new();
    let sniff_limit = 8192usize;
    let mut sniff_checked = false;
    let mut sniffed_content_type = None;

    while let Some(chunk) = field.chunk().await.map_err(MultipartError::from)? {
        add_bytes(request_bytes, chunk.len() as u64, conf.max_request_bytes)?;
        size += chunk.len() as u64;
        if size > max_size {
            return Err(MultipartError::too_large(
                name,
                format!("file exceeds {max_size} bytes"),
            ));
        }

        if rule.sniff.is_some() && sniff_prefix.len() < sniff_limit {
            let remaining = sniff_limit - sniff_prefix.len();
            sniff_prefix.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
            if !sniff_checked && !sniff_prefix.is_empty() {
                if let Some(kind) = infer::get(&sniff_prefix) {
                    let mime = kind.mime_type().to_ascii_lowercase();
                    rule.validate_sniff(name, Some(&mime))?;
                    sniffed_content_type = Some(mime);
                    sniff_checked = true;
                }
            }
        }

        if let Some((_, file)) = &mut temp {
            file.write_all(&chunk).await?;
        } else if memory.len() as u64 + chunk.len() as u64 > conf.memory_threshold_bytes {
            let (temp_path, mut file) = create_temp_file(temp_dir.as_deref()).await?;
            file.write_all(&memory).await?;
            file.write_all(&chunk).await?;
            memory.clear();
            temp = Some((temp_path, file));
        } else {
            memory.extend_from_slice(&chunk);
        }
    }

    if rule.sniff.is_some() && !sniff_checked {
        let detected = infer::get(&sniff_prefix).map(|kind| kind.mime_type().to_ascii_lowercase());
        rule.validate_sniff(name, detected.as_deref())?;
        sniffed_content_type = detected;
    }

    let data = if let Some((temp_path, mut file)) = temp {
        file.flush().await?;
        UploadedFileData::Temp { path: temp_path }
    } else {
        UploadedFileData::Memory(Bytes::from(memory))
    };

    Ok(UploadedFile(Arc::new(UploadedFileInner {
        field_name: name.to_string(),
        file_name,
        content_type,
        sniffed_content_type,
        size,
        data,
    })))
}

fn add_bytes(total: &mut u64, next: u64, max: u64) -> Result<(), MultipartError> {
    *total = total.saturating_add(next);
    if *total > max {
        return Err(MultipartError::too_large(
            "body",
            format!("multipart request exceeds {max} bytes"),
        ));
    }
    Ok(())
}

fn detect_file_mime(file: &UploadedFile) -> Result<Option<String>, MultipartError> {
    let mut prefix = Vec::new();
    match &file.0.data {
        UploadedFileData::Memory(bytes) => {
            prefix.extend_from_slice(&bytes[..bytes.len().min(8192)]);
        }
        UploadedFileData::Temp { path } => {
            use std::io::Read as _;
            let mut reader = std::fs::File::open(path)?;
            let mut buf = [0u8; 8192];
            let read = reader.read(&mut buf)?;
            prefix.extend_from_slice(&buf[..read]);
        }
    }
    Ok(infer::get(&prefix).map(|kind| kind.mime_type().to_ascii_lowercase()))
}

fn state_relative_temp_dir(project_dir: &Path, dir: &str) -> Result<PathBuf, MultipartError> {
    let path = PathBuf::from(dir);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(project_dir.join(path))
    }
}

async fn create_temp_file(
    dir: Option<&Path>,
) -> Result<(TempPath, tokio::fs::File), MultipartError> {
    let named = match dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)?;
            tempfile::Builder::new()
                .prefix("vyuh-upload-")
                .tempfile_in(dir)?
        }
        None => tempfile::Builder::new().prefix("vyuh-upload-").tempfile()?,
    };
    let temp_path = named.into_temp_path();
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(<TempPath as AsRef<Path>>::as_ref(&temp_path))
        .await?;
    Ok((temp_path, file))
}

impl<T> crate::callables::IntoArgPart for MultipartForm<T>
where
    T: JsonSchema + MultipartData + Send + 'static,
{
    fn into_arg_part() -> crate::callables::ArgPart {
        let spec = T::multipart_spec();
        crate::callables::ArgPart::Composite(vec![
            crate::callables::ArgPart::BodyWith {
                schema: crate::callables::TypeSchema::wrap_unvalidated::<T>(),
                content_type: Cow::Borrowed("multipart/form-data"),
                multipart: Some(MultipartApiSpec::from(&spec)),
            },
            crate::callables::ArgPart::Response(vec![crate::callables::ReturnSpec::error(
                400,
                "Bad request.",
            )]),
        ])
    }
}

impl<T> crate::callables::IntoArgPart for Valid<MultipartForm<T>>
where
    T: JsonSchema
        + MultipartData
        + crate::validation::Validate
        + crate::validation::ValidationSchema
        + Send
        + 'static,
{
    fn into_arg_part() -> crate::callables::ArgPart {
        let spec = T::multipart_spec();
        crate::callables::ArgPart::Composite(vec![
            crate::callables::ArgPart::BodyWith {
                schema: crate::callables::TypeSchema::wrap_valid::<T>(),
                content_type: Cow::Borrowed("multipart/form-data"),
                multipart: Some(MultipartApiSpec::from(&spec)),
            },
            crate::callables::ArgPart::Response(vec![
                crate::callables::ReturnSpec::error(400, "Bad request."),
                crate::callables::ReturnSpec::error(422, "Validation failed."),
            ]),
        ])
    }
}

impl crate::callables::IntoArgPart for MultipartMap {
    fn into_arg_part() -> crate::callables::ArgPart {
        crate::callables::ArgPart::Composite(vec![
            crate::callables::ArgPart::Body(
                crate::callables::TypeSchema::binary_body(),
                Cow::Borrowed("multipart/form-data"),
            ),
            crate::callables::ArgPart::Response(vec![crate::callables::ReturnSpec::error(
                400,
                "Bad request.",
            )]),
        ])
    }
}
