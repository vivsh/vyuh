use std::{collections::BTreeMap, future::Future};

use serde_json::json;

use super::*;

tokio::task_local! {
    static DOCUMENTS: BTreeMap<String, Result<Vec<u8>, OAuthIssueError>>;
}

pub(in crate::auth::oauth_issue) async fn documents<'a, T>(
    items: impl IntoIterator<Item = (&'a str, Result<Value, OAuthIssueError>)>,
    work: impl Future<Output = T>,
) -> T {
    let documents = items
        .into_iter()
        .map(|(url, result)| {
            (
                url.to_owned(),
                result.map(|value| value.to_string().into_bytes()),
            )
        })
        .collect();
    DOCUMENTS.scope(documents, work).await
}

pub(super) fn document(url: &Url) -> Option<Result<Vec<u8>, OAuthIssueError>> {
    DOCUMENTS
        .try_with(|docs| docs.get(url.as_str()).cloned().unwrap_or(Err(BAD)))
        .ok()
}

/// Rejects invalid IDs and special-use destinations without DNS or external services.
#[test]
fn restricts_urls_and_ip_addresses() -> Result<(), std::net::AddrParseError> {
    for candidate in [
        "http://example.com/client.json",
        "https://user@example.com/client.json",
        "https://@example.com/client.json",
        "https://example.com/",
        "https://example.com/a/%2E%2e/client.json",
        "https://example.com/client.json?a=b",
        "https://example.com/client.json#fragment",
        "https://127.0.0.1/client.json",
        "https://[::1]/client.json",
        "https://2130706433/client.json",
        "https://example.com./client.json",
        "https://example.com:0/client.json",
        "https://example.com/client.json ",
    ] {
        assert!(document_url(candidate).is_none(), "{candidate}");
    }
    for candidate in [
        "https://example.com/client.json",
        "https://example.com:443/client.json",
        "https://example.com:8443/client.json",
    ] {
        assert!(document_url(candidate).is_some(), "{candidate}");
    }
    for candidate in [
        "0.1.2.3",
        "10.1.2.3",
        "100.64.0.1",
        "127.0.0.1",
        "169.254.169.254",
        "172.31.0.1",
        "192.168.1.1",
        "192.0.0.1",
        "198.18.0.1",
        "198.51.100.2",
        "224.0.0.1",
        "::",
        "::1",
        "::ffff:127.0.0.1",
        "64:ff9b::a9fe:a9fe",
        "fd00::1",
        "fe80::1",
        "2001:db8::1",
        "2002:7f00:1::1",
        "3fff::1",
    ] {
        assert!(!public_address(candidate.parse()?), "{candidate}");
    }
    for candidate in ["1.1.1.1", "2606:4700:4700::1111"] {
        assert!(public_address(candidate.parse()?), "{candidate}");
    }
    Ok(())
}

/// The actual connector resolver denies DNS answers for the local host.
#[tokio::test]
async fn connector_rejects_loopback_dns() -> Result<(), String> {
    use reqwest::dns::Resolve;
    let name: dns::Name = "localhost".parse().map_err(|_| "invalid DNS test name")?;
    assert!(PublicDns.resolve(name).await.is_err());
    Ok(())
}

/// HTTP redirects, non-JSON and oversized streamed bodies fail before metadata parsing.
#[tokio::test]
async fn bounds_http_response() -> Result<(), axum::http::Error> {
    use axum::http::Response;
    let redirected = Response::builder()
        .status(302)
        .header(header::LOCATION, "http://127.0.0.1/metadata")
        .header(header::CONTENT_TYPE, "application/json")
        .body("{}")?;
    assert!(matches!(
        read_json(redirected.into()).await,
        Err(OAuthIssueError::InvalidClient)
    ));
    let not_json = Response::builder()
        .header(header::CONTENT_TYPE, "text/html")
        .body("{}")?;
    assert!(matches!(
        read_json(not_json.into()).await,
        Err(OAuthIssueError::InvalidClient)
    ));
    let chunks = [Ok::<_, io::Error>(vec![b'x'; LIMIT]), Ok(vec![b'x'])];
    let body = reqwest::Body::wrap_stream(futures::stream::iter(chunks));
    let oversized = Response::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)?;
    assert!(matches!(
        read_json(oversized.into()).await,
        Err(OAuthIssueError::InvalidClient)
    ));
    let valid = Response::builder()
        .header(header::CONTENT_TYPE, "application/jwk-set+json")
        .body("{}")?;
    assert!(
        read_json(valid.into())
            .await
            .is_ok_and(|body| body == b"{}")
    );
    Ok(())
}

/// Signed rules reject conflicting metadata, untrusted keys and identity changes.
#[tokio::test]
async fn restricts_document_metadata() -> Result<(), String> {
    let id = "https://example.com/client.json";
    let callback = "https://example.com/callback";
    let policy = OAuthCimdClient::new(id, "Valid client", OAuthCimdAuth::PrivateKeyJwt)
        .redirect_uri(callback);
    let url = document_url(id).ok_or("invalid test document URL")?;
    let deadline = Instant::now() + DEADLINE;
    let base = json!({"client_id": id, "redirect_uris":[callback],
        "token_endpoint_auth_methods_supported": ["none", "private_key_jwt"],
        "token_endpoint_auth_method": "private_key_jwt",
        "grant_types": ["authorization_code", "refresh_token", "custom_grant"],
        "jwks_uri": "https://example.com/jwks.json"});
    assert!(parse(base.to_string().as_bytes(), &policy, &url, deadline).is_ok());
    for (key, invalid) in [
        ("client_id", json!("https://example.com:443/client.json")),
        (
            "redirect_uris",
            json!(["https://example.com/another-callback"]),
        ),
        ("token_endpoint_auth_method", json!("client_secret_basic")),
        ("token_endpoint_auth_methods_supported", json!(["none"])),
        (
            "token_endpoint_auth_methods_supported",
            json!(["private_key_jwt", "client_secret_post"]),
        ),
        ("jwks_uri", json!("https://other.example.com/jwks.json")),
        ("jwks_uri", json!("https://example.com:8443/jwks.json")),
        ("jwks", json!({"keys": []})),
        ("client_secret", Value::Null),
        ("grant_types", json!(["refresh_token"])),
        ("response_types", json!(["token"])),
    ] {
        let mut denied = base.clone();
        denied[key] = invalid;
        assert!(
            parse(denied.to_string().as_bytes(), &policy, &url, deadline).is_err(),
            "{key}"
        );
    }
    let duplicated = br#"{"client_id":"https://example.com/client.json","client_id":"https://example.com/client.json","redirect_uris":["https://example.com/callback"]}"#;
    assert!(parse(duplicated, &policy, &url, deadline).is_err());
    Ok(())
}
