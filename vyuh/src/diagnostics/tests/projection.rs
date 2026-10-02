use crate::{SiteError, auth::AuthError, bundles::BundleError, conf::ConfError};

/// Duplicate routes have an actionable, stable terminal representation.
#[test]
fn duplicate_route_snapshot() {
    let error = SiteError::BundleError(BundleError::DuplicateRoutePathMethod {
        path: "/items".into(),
        methods: "GET".into(),
    });
    assert_eq!(
        error.to_string(),
        concat!(
            "Site assembly failed:\n\n1. A route is registered more than once.",
            "\n   Route: GET /items",
            "\n   Paths differing only by one terminal slash use the same internal routing path.",
            "\n   hint: Keep one registration for this method and normalized path, or give the operations distinct paths."
        )
    );
    assert_eq!(error.to_string(), format!("{error:?}"));
    assert_eq!(error.diagnostics().len(), 1);
}

/// Typed auth failures select distinct remedies instead of one generic prefix hint.
#[test]
fn auth_remedies_follow_variants() {
    let duplicate = SiteError::ConfError(ConfError::Auth(AuthError::DuplicateProvider(
        "tokens".into(),
    )));
    let reserved = SiteError::ConfError(ConfError::Auth(AuthError::ReservedProviderId(
        "vyuh-token".into(),
    )));
    assert!(
        duplicate
            .to_string()
            .contains("Register each provider name once")
    );
    assert!(!duplicate.to_string().contains("reserved"));
    assert!(reserved.to_string().contains("reserved 'vyuh-' prefix"));
    assert!(std::error::Error::source(&duplicate).is_some());
}

/// Existing nested validation lists retain order and produce separate diagnostics.
#[test]
fn lists_flatten_in_order() {
    let error = SiteError::ConfError(ConfError::Many(vec![
        ConfError::Cache(crate::cache::CacheError::MissingDefaultProvider),
        ConfError::Many(vec![ConfError::Auth(AuthError::DuplicateProvider(
            "tokens".into(),
        ))]),
    ]));
    let diagnostics = error.diagnostics();
    assert_eq!(diagnostics.len(), 2);
    assert!(diagnostics.first().unwrap().summary.contains("Cache"));
    assert!(error.to_string().contains("\n2. Authentication"));
    let bundle = BundleError::ErrorList(vec![BundleError::ErrorList(vec![
        BundleError::DuplicateRouteName {
            name: "items".into(),
        },
    ])]);
    assert!(!bundle.to_string().contains("ErrorList"));
    assert!(bundle.to_string().contains("unique name"));
}

/// Secret-bearing fields omit their values, even when an error repeats them.
#[test]
fn secret_fields_omit_values() {
    let error = SiteError::ConfError(ConfError::InvalidValue {
        field: "secret_key".into(),
        reason: "hunter2 is too short".into(),
        expected: Some("not hunter2".into()),
    });
    assert!(!error.to_string().contains("hunter2"));
    assert!(error.to_string().contains("secret_key"));
}

/// Opaque initialization sources retain useful detail without duplicate causes.
#[test]
fn service_context_and_causes() {
    let error = SiteError::ServiceError(crate::services::ServiceError::Initialization {
        service: "app::LocalApp",
        source: Box::new(crate::services::ServiceError::CallError(
            crate::callables::CallError::Other(Box::new(std::io::Error::other(
                "vault permission denied; password=hidden",
            ))),
        )),
    });
    let rendered = error.to_string();
    assert!(rendered.contains("app::LocalApp"));
    assert_eq!(rendered.matches("vault permission denied").count(), 1);
    assert!(!rendered.contains("hidden"));
}

/// The command-owned terminal representation is not wrapped as an assembly failure.
#[test]
fn command_output_is_preserved() {
    let error = SiteError::CommandError(crate::commands::CommandError::Exit(
        "Error: migration needs input\n  hint: supply decisions".into(),
    ));
    assert!(!error.to_string().contains("Site assembly"));
    assert_eq!(
        format!("{error:?}"),
        "migration needs input\n  hint: supply decisions"
    );
}

/// Every major assembly subsystem provides a summary and a concrete next step.
#[test]
fn subsystem_summaries() {
    let cases = [
        (
            SiteError::TaskRuntimeError(crate::tasks::TaskRuntimeError::UnknownLane("mail".into())),
            "Task initialization failed.",
            "TaskLaneConf",
        ),
        (
            SiteError::ConfError(ConfError::Cache(
                crate::cache::CacheError::DuplicateProvider,
            )),
            "Cache configuration failed.",
            "CacheConf::default()",
        ),
        (
            SiteError::LoggingError(crate::logging::LoggingError::DuplicateRuleName(
                "file".into(),
            )),
            "Logging initialization failed.",
            "unique name",
        ),
        (
            SiteError::TemplateError(crate::templates::TemplateError::NotFound(
                "page.html".into(),
            )),
            "Template initialization failed.",
            "embedded template",
        ),
        (
            SiteError::EmitterError(crate::emitters::EmitterError::AlreadyExists),
            "Emitter initialization failed.",
            "only once",
        ),
        (
            SiteError::AuthBuildError(crate::auth::AuthBuildError::WorkerFailure),
            "Authentication initialization worker failed.",
            "worker failure",
        ),
        (
            SiteError::BundleError(BundleError::DocGen("schema mismatch".into())),
            "OpenAPI generation failed.",
            "schema",
        ),
    ];
    for (error, summary, hint) in cases {
        let diagnostics = error.diagnostics();
        let diagnostic = diagnostics.first().unwrap();
        assert_eq!(diagnostic.summary, summary);
        assert!(diagnostic.hint.as_deref().unwrap().contains(hint));
        assert_eq!(error.to_string(), format!("{error:?}"));
    }
}

/// Missing migration support points to the feature without leaking asset contents.
#[cfg(not(feature = "migrations"))]
#[test]
fn schema_feature_hint() {
    let error =
        SiteError::SchemaAsset(crate::schema_assets::SchemaAssetError::MigrationsDisabled {
            path: "schema/tables.sql".into(),
        });
    assert!(
        error
            .to_string()
            .contains("Enable Vyuh's migrations feature")
    );
    assert!(error.to_string().contains("schema/tables.sql"));
}

/// Framework-owned SQL text remains in the source, not the assembly projection.
#[test]
fn database_payload_is_omitted() {
    let error = SiteError::DatabaseError(crate::db::DbError::Mock {
        operation: "connect",
        reason: "SELECT 'private-data'".into(),
    });
    assert!(error.to_string().contains("mock_error"));
    assert!(!error.to_string().contains("private-data"));
    assert!(std::error::Error::source(&error).is_some());
}

/// Cyclic third-party source chains stop at a bounded depth.
#[test]
fn cyclic_causes_are_bounded() {
    #[derive(Debug)]
    struct Cyclic;
    impl std::fmt::Display for Cyclic {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("cyclic cause")
        }
    }
    impl std::error::Error for Cyclic {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self)
        }
    }
    let diagnostic = super::D::new("Failure.", "Inspect the source.").causes(&Cyclic);
    assert_eq!(
        diagnostic.details,
        ["cyclic cause", "[cause chain truncated]"]
    );
}

/// Migration registration errors retain distinct root and namespace context.
#[cfg(feature = "migrations")]
#[test]
fn migration_registration_context() {
    let error = SiteError::BundleError(BundleError::Migration(std::sync::Arc::new(
        crate::db::MigrationError::DuplicateRoot,
    )));
    assert!(
        error
            .to_string()
            .contains("duplicate root migration source")
    );
    assert!(error.to_string().contains("one root migration history"));
}

/// MCP declarations keep their supplied context and subsystem-specific remedy.
#[cfg(feature = "mcp")]
#[test]
fn mcp_registration_context() {
    let error = SiteError::BundleError(BundleError::Mcp("unclaimed tool: search".into()));
    assert!(error.to_string().contains("unclaimed tool: search"));
    assert!(error.to_string().contains("tool ownership"));
}
