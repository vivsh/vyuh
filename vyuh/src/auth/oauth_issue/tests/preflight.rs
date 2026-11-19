use super::*;

/// Parses one exact metadata parameter and rejects ambiguous or malformed challenges.
#[test]
fn challenge_metadata() {
    let expected = "https://example.com/.well-known/oauth-protected-resource/mcp";
    assert!(callback_metadata(
        &format!(
            "error_description=\"Check the URL, then retry\", resource_metadata=\"{expected}\""
        ),
        expected
    ));
    for value in [
        format!("resource_metadata=\"{expected}\", resource_metadata=\"{expected}\""),
        format!("resource_metadata=\"{expected}\", "),
        format!("resource_metadata=\"{expected}"),
        format!("resource_metadata={expected}"),
        format!("error={expected}"),
    ] {
        assert!(!callback_metadata(&value, expected));
    }
}

#[cfg(feature = "sqlite")]
pub(super) use local::{fixture_site, route};

#[cfg(feature = "sqlite")]
mod local {
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::ServiceExt;

    use crate::{
        auth::{OAuthDcr, OAuthDcrAdmission, oauth_issue::tests as flow},
        bundles, db,
    };

    use super::*;

    const ORIGIN: &str = "https://app.example.com";
    const MCP: &str = "https://app.example.com/mcp";
    const CALLBACK: &str = "https://client.example.com/callback";

    tokio::task_local! {
        static COMMAND_SITE: Site;
    }

    pub(in crate::auth::oauth_issue::preflight) fn fixture_site() -> Option<Site> {
        COMMAND_SITE.try_with(Clone::clone).ok()
    }

    /// Supplies the same exact resource, callback and scope configured in the fixture.
    fn args(mode: &str) -> PreflightArgs {
        PreflightArgs {
            resource: MCP.into(),
            mode: mode.into(),
            redirect_uri: CALLBACK.into(),
            client_id: (mode == SIGNED).then(|| flow::cimd_flow::SIGNED.into()),
            token_auth_method: (mode == DCR).then(|| "none".into()),
            scopes: vec!["notes:read".into()],
            require_refresh: true,
            json: true,
        }
    }

    /// Builds an application through the same bundle and command wiring used by consumers.
    async fn site(mode: &str) -> Result<Site, String> {
        let database =
            db::DbConf::from_url("sqlite::memory:").map_err(|error| error.to_string())?;
        let issuer = flow::issuer_base();
        let issuer = if mode == SIGNED {
            issuer.cimd_client(flow::cimd_flow::rule(
                flow::cimd_flow::SIGNED,
                OAuthCimdAuth::PrivateKeyJwt,
            ))
        } else {
            issuer.dynamic_registration(
                OAuthDcr::new("Approved", OAuthDcrAdmission::Unauthenticated)
                    .redirect_uri(CALLBACK)
                    .method(OAuthDcrMethod::None)
                    .scopes([Scope::of("notes:read")])
                    .allow_refresh(),
            )
        };
        Site::build(
            flow::conf_with(database, issuer),
            flow::bundle().merge(bundles::bundle([mcp_preflight()])),
        )
        .await
        .map_err(|error| error.to_string())
    }

    /// Sends the proposed requests through the real MCP and discovery routes without opening sockets.
    pub(in crate::auth::oauth_issue::preflight) async fn route(
        site: &Site,
        request: RequestBuilder,
    ) -> Result<Response, &'static str> {
        let request = request.build().map_err(|_| "bad_test_request")?;
        let path = request.url().path();
        let method = request.method();
        if !matches!(
            (method.as_str(), path),
            ("POST", "/mcp")
                | ("GET", METADATA_PATH)
                | ("GET", "/.well-known/oauth-protected-resource/mcp")
        ) || request.url().origin().ascii_serialization() != ORIGIN
            || request.headers().contains_key(header::AUTHORIZATION)
            || request.headers().contains_key(header::COOKIE)
        {
            return Err("unsafe_test_probe");
        }
        let body = request
            .body()
            .and_then(reqwest::Body::as_bytes)
            .unwrap_or_default();
        if method == reqwest::Method::POST
            && body != br#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#
        {
            return Err("unexpected_rpc");
        }
        let mut incoming = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .body(axum::body::Body::from(body.to_vec()))
            .map_err(|_| "bad_router_request")?;
        incoming.headers_mut().clone_from(request.headers());
        let (parts, body) = site
            .router()
            .oneshot(incoming)
            .await
            .map_err(|_| "router_error")?
            .into_parts();
        let body = axum::body::to_bytes(body, 32 * 1024)
            .await
            .map_err(|_| "router_body_error")?;
        Ok(axum::http::Response::from_parts(parts, body).into())
    }

    fn failed(report: &PreflightReport, id: &str, detail: &str) -> bool {
        report.checks.iter().any(|check| {
            check.id == id && check.status == CheckStatus::Fail && check.detail == detail
        }) && report.result().is_err()
    }

    /// A signed-only deployment resolves its document and key through the configured client rule.
    #[tokio::test]
    async fn signed_probe() -> Result<(), String> {
        let site = site(SIGNED).await?;
        let args = args(SIGNED);
        let report = flow::cimd_flow::fixture(
            &flow::cimd_flow::OLD,
            inspect(&site, &args, |request| route(&site, request)),
        )
        .await;
        assert!(
            report.result().is_ok(),
            "{}",
            report.render(false).map_err(|e| e.to_string())?
        );
        assert!(
            report
                .checks
                .iter()
                .all(|check| check.status == CheckStatus::Pass)
        );
        let (id, invalid) = (
            flow::cimd_flow::SIGNED,
            super::super::super::OAuthIssueError::Unavailable,
        );
        let report = cimd::tests::documents(
            [(id, Err(invalid))],
            inspect(&site, &args, |request| route(&site, request)),
        )
        .await;
        assert!(failed(
            &report,
            DOCUMENT,
            "cimd_document_unavailable_or_invalid"
        ));
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == KEYS && check.status == CheckStatus::Skipped)
        );
        Ok(())
    }

    /// Invalid local targets and callbacks are reported before any outbound request.
    #[tokio::test]
    async fn local_rejection_and_cli() -> Result<(), String> {
        let site = site(SIGNED).await?;
        let calls = AtomicUsize::new(0);
        let send = |_| {
            calls.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Err("unexpected_egress"))
        };
        let mut args = args(SIGNED);
        args.redirect_uri = "https://client.example.com/wrong".into();
        let report = inspect(&site, &args, send).await;
        assert!(failed(&report, LOCAL, "callback_not_trusted"));
        args.resource = "https://example.com/mcp?secret=hidden".into();
        let report = inspect(&site, &args, send).await;
        assert!(failed(&report, LOCAL, "resource_not_configured"));
        let json = report.render(true).map_err(|e| e.to_string())?;
        assert!(!json.contains("secret") && !json.contains("hidden"));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        let name = "mcp:preflight";
        site.execute_command(name, &["--help"])
            .await
            .map_err(|e| e.to_string())?;
        assert!(
            site.execute_command(name, &["--mode", "dcr"])
                .await
                .is_err()
        );
        let error = site
            .execute_command(
                name,
                &[
                    "--resource",
                    "https://example.com/mcp",
                    "--mode",
                    "signed-cimd",
                    "--client-id",
                    flow::cimd_flow::SIGNED,
                    "--redirect-uri",
                    CALLBACK,
                    "--scopes",
                    "notes:read",
                    "--require-refresh",
                    "--json",
                ],
            )
            .await
            .err()
            .ok_or("unknown resource unexpectedly succeeded")?;
        assert!(error.to_string().contains("MCP preflight checks failed"));
        Ok(())
    }

    /// DCR checks only the three deployment probe routes, even when registration is enabled.
    #[tokio::test]
    async fn dcr_probe() -> Result<(), String> {
        let site = site(DCR).await?;
        let count = AtomicUsize::new(0);
        let report = inspect(&site, &args(DCR), |request| {
            count.fetch_add(1, Ordering::Relaxed);
            route(&site, request)
        })
        .await;
        assert_eq!(count.load(Ordering::Relaxed), 3);
        assert!(
            report
                .checks
                .iter()
                .all(|check| check.status == CheckStatus::Pass)
        );
        assert_eq!(
            report.checks.last().ok_or("missing check")?.id,
            REGISTRATION
        );
        Ok(())
    }

    /// Executes the registered command successfully using the CLI flags and real discovery routes.
    #[tokio::test]
    async fn command_success() -> Result<(), String> {
        let site = site(DCR).await?;
        COMMAND_SITE
            .scope(
                site.clone(),
                site.execute_command(
                    "mcp:preflight",
                    &[
                        "--resource",
                        MCP,
                        "--mode",
                        DCR,
                        "--redirect-uri",
                        CALLBACK,
                        "--token-auth-method",
                        "none",
                        "--scopes",
                        "notes:read",
                        "--require-refresh",
                        "--json",
                    ],
                ),
            )
            .await
            .map_err(|error| error.to_string())
    }

    /// Rejects a broken challenge, issuer substitution and missing PKCE without leaking upstream data.
    #[tokio::test]
    async fn discovery_failures() -> Result<(), String> {
        let site = site(SIGNED).await?;
        let args = args(SIGNED);
        for (path, stage, reason) in [
            ("/mcp", CHALLENGE, "missing_bearer_challenge"),
            ("issuer", ISSUER, "issuer_or_endpoint_mismatch"),
            ("pkce", ISSUER, "s256_not_advertised"),
        ] {
            let report =
                inspect(&site, &args, |request| break_response(&site, request, path)).await;
            assert!(
                failed(&report, stage, reason),
                "{}",
                report.render(false).map_err(|e| e.to_string())?
            );
            assert!(
                !report
                    .render(true)
                    .map_err(|e| e.to_string())?
                    .contains("sample-credential")
            );
        }
        Ok(())
    }

    /// Corrupts one discovery response while forwarding the other requests to the actual router.
    async fn break_response(
        site: &Site,
        request: RequestBuilder,
        failure: &str,
    ) -> Result<Response, &'static str> {
        let path = request
            .try_clone()
            .ok_or("missing probe")?
            .build()
            .map_err(|_| "bad probe")?
            .url()
            .path()
            .to_owned();
        let response = route(site, request).await?;
        if path == "/mcp" && failure == path {
            return axum::http::Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body("sample-credential")
                .map(Into::into)
                .map_err(|_| "bad fixture");
        }
        if path != METADATA_PATH {
            return Ok(response);
        }
        let mut json: Value =
            serde_json::from_slice(&response.bytes().await.map_err(|_| "bad body")?)
                .map_err(|_| "invalid json")?;
        let (key, value) = if failure == "issuer" {
            (
                "issuer",
                json!("https://other.example.com/sample-credential"),
            )
        } else {
            ("code_challenge_methods_supported", json!(["plain"]))
        };
        json[key] = value;
        axum::http::Response::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .body(json.to_string())
            .map(Into::into)
            .map_err(|_| "bad fixture")
    }
}
