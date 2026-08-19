//! Static MCP resource declarations and validation.

use mime::Mime;
use serde_json::{Value, json};

use super::McpError;

/// Optional MCP Apps metadata associated with a static resource.
#[derive(Clone, Debug, Default)]
pub struct McpUiResourceMeta {
    prefers_border: Option<bool>,
}

impl McpUiResourceMeta {
    /// Requests a border around the resource when the host supports that hint.
    pub fn prefers_border(mut self, value: bool) -> Self {
        self.prefers_border = Some(value);
        self
    }

    fn value(&self) -> Value {
        json!({"ui": {"prefersBorder": self.prefers_border}})
    }
}

/// Immutable text content exposed through the MCP resources protocol.
#[derive(Clone, Debug)]
pub struct McpResource {
    uri: String,
    mime_type: String,
    text: String,
    ui: Option<McpUiResourceMeta>,
}

impl McpResource {
    /// Creates one static UTF-8 resource declaration.
    ///
    /// URI and MIME syntax are validated while the enclosing site is built.
    pub fn text(
        uri: impl Into<String>,
        mime_type: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            uri: uri.into(),
            mime_type: mime_type.into(),
            text: text.into(),
            ui: None,
        }
    }

    /// Adds MCP Apps UI metadata to this static resource.
    pub fn ui(mut self, metadata: McpUiResourceMeta) -> Self {
        self.ui = Some(metadata);
        self
    }

    pub(crate) fn validate(&self) -> Result<(), McpError> {
        validate_resource_uri(&self.uri)?;
        validate_mime(&self.mime_type)?;
        if is_ui_uri(&self.uri) {
            validate_ui_uri(&self.uri)?;
        }
        Ok(())
    }

    pub(crate) fn definition(&self, name: String) -> ResourceDefinition {
        ResourceDefinition {
            name,
            uri: self.uri.clone(),
            mime_type: self.mime_type.clone(),
            text: self.text.clone(),
            ui: self.ui.clone(),
        }
    }

    /// Returns the immutable resource URI used as the registry key.
    pub(crate) fn uri(&self) -> &str {
        &self.uri
    }
}

/// One static resource retained by an MCP service runtime.
#[derive(Clone, Debug)]
pub(crate) struct ResourceDefinition {
    pub(crate) name: String,
    pub(crate) uri: String,
    pub(crate) mime_type: String,
    pub(crate) text: String,
    ui: Option<McpUiResourceMeta>,
}

impl ResourceDefinition {
    /// Returns the standard resource catalog representation.
    pub(crate) fn list_value(&self) -> Value {
        json!({
            "uri": self.uri,
            "name": self.name,
            "mimeType": self.mime_type,
            "size": self.text.len(),
        })
    }

    /// Returns the standard text resource contents representation.
    pub(crate) fn content_value(&self) -> Value {
        let mut value = json!({
            "uri": self.uri,
            "mimeType": self.mime_type,
            "text": self.text,
        });
        if let Some(metadata) = &self.ui {
            value["_meta"] = metadata.value();
        }
        value
    }

    /// Returns whether this definition is an MCP Apps HTML resource.
    pub(crate) fn is_mcp_app_html(&self) -> bool {
        is_mcp_app_html(&self.mime_type)
    }
}

/// Validates an absolute MCP resource URI without a fragment.
pub(crate) fn validate_resource_uri(uri: &str) -> Result<(), McpError> {
    let value = url::Url::parse(uri)
        .map_err(|error| McpError::Config(format!("invalid MCP resource URI '{uri}': {error}")))?;
    if value.scheme().is_empty() || value.fragment().is_some() {
        return Err(McpError::Config(format!(
            "MCP resource URI '{uri}' must be absolute and have no fragment"
        )));
    }
    Ok(())
}

/// Validates a strict MCP Apps `ui://` resource URI.
pub(crate) fn validate_ui_uri(uri: &str) -> Result<(), McpError> {
    validate_resource_uri(uri)?;
    let value = url::Url::parse(uri).map_err(|error| {
        McpError::Config(format!("invalid MCP UI resource URI '{uri}': {error}"))
    })?;
    let clean = value.scheme() == "ui"
        && value.host_str().is_some_and(|host| !host.is_empty())
        && !value.path().is_empty()
        && value.path() != "/"
        && value.username().is_empty()
        && value.password().is_none()
        && value.port().is_none()
        && value.query().is_none()
        && value.fragment().is_none();
    if clean {
        Ok(())
    } else {
        Err(McpError::Config(format!(
            "invalid MCP Apps UI resource URI '{uri}'"
        )))
    }
}

/// Returns whether a MIME value is the MCP Apps HTML media type.
pub(crate) fn is_mcp_app_html(value: &str) -> bool {
    value.parse::<Mime>().is_ok_and(|mime| {
        mime.essence_str() == "text/html"
            && mime
                .get_param("profile")
                .is_some_and(|profile| profile.as_str() == "mcp-app")
    })
}

fn is_ui_uri(uri: &str) -> bool {
    url::Url::parse(uri).is_ok_and(|value| value.scheme() == "ui")
}

fn validate_mime(value: &str) -> Result<(), McpError> {
    value
        .parse::<Mime>()
        .map(|_| ())
        .map_err(|error| McpError::Config(format!("invalid MCP resource MIME '{value}': {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies normal static resources accept arbitrary syntactically valid MIME types.
    #[test]
    fn accepts_generic_resource() {
        let resource = McpResource::text("https://example.test/readme", "text/plain", "hello");
        assert!(resource.validate().is_ok());
    }

    /// Verifies UI resources require a strict ui URI while generic MIME types remain valid.
    #[test]
    fn validates_mcp_apps_resource() {
        let resource = McpResource::text(
            "ui://widget/member-card.html",
            "text/html;profile=mcp-app",
            "<main></main>",
        )
        .ui(McpUiResourceMeta::default().prefers_border(true));
        assert!(resource.validate().is_ok());
        assert!(validate_ui_uri("ui://widget/member-card.html?preview=1").is_err());
        let invalid = McpResource::text("ui://widget", "text/plain", "x");
        assert!(invalid.validate().is_err());
    }

    /// Verifies malformed MIME values and fragments are rejected at site construction.
    #[test]
    fn rejects_invalid_resource_syntax() {
        let resource = McpResource::text("https://example.test/readme#part", "invalid mime", "x");
        assert!(resource.validate().is_err());
    }
}
