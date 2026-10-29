use super::*;

const MCP: Audience = Audience::new("https://api.example.com/mcp");

/// Required scopes are automatically advertised by the protected resource.
#[test]
fn required_scopes_are_advertised() {
    let resource = OAuthResource::new(MCP.as_str()).require_scopes(["mcp"]);
    assert!(resource.advertised.contains("mcp"));
    assert!(resource.validate().is_ok());
}

/// A complete resource policy accepts multiple valid upstream scopes.
#[test]
fn accepts_multiple_advertised_and_required_scopes() {
    let result = OAuthResourceServer::discovery("https://auth.example.com").resource(
        MCP,
        OAuthResource::new(MCP.as_str())
            .advertise_scopes(["mcp", "mcp.read"])
            .require_scopes(["mcp", "mcp.read"]),
    );
    assert!(result.validate().is_ok());
}

/// Duplicate audiences are rejected during terminal provider validation.
#[test]
fn rejects_duplicate_resource_audiences() {
    let result = OAuthResourceServer::discovery("https://auth.example.com")
        .resource(MCP, OAuthResource::new(MCP.as_str()))
        .resource(MCP, OAuthResource::new(MCP.as_str()))
        .validate();
    assert!(result.is_err());
}

/// Public algorithm choices map exactly to the identifiers accepted by Huskarl.
#[test]
fn maps_supported_algorithms() {
    let values = [
        (OAuthJwtAlgorithm::Rs256, "RS256"),
        (OAuthJwtAlgorithm::Rs384, "RS384"),
        (OAuthJwtAlgorithm::Rs512, "RS512"),
        (OAuthJwtAlgorithm::Es256, "ES256"),
        (OAuthJwtAlgorithm::Es384, "ES384"),
        (OAuthJwtAlgorithm::EdDsa, "EdDSA"),
    ];
    for (algorithm, expected) in values {
        assert_eq!(algorithm.name(), expected);
    }
}

/// The safe default mapper never copies upstream OAuth scopes into the user.
#[tokio::test]
async fn default_mapper_grants_no_application_scopes() -> Result<(), AuthError> {
    let claims = OAuthClaims {
        subject: "oauth-user".to_string(),
        issuer: "https://auth.example.com".to_string(),
        audiences: vec![MCP.as_str().to_string()],
        scopes: BTreeSet::from(["upstream:read".to_string()]),
        raw: serde_json::Value::Null,
    };
    let site = crate::Site::build(
        crate::SiteConf::default().log_init(false),
        crate::bundles::Bundle::default(),
    )
    .await
    .map_err(|error| AuthError::Internal(error.to_string()))?;
    let user = OAuthIdentityMapper::map(&SubjectMapper, &site, &claims).await?;
    assert!(user.scopes().is_empty());
    Ok(())
}
