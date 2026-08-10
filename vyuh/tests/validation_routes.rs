use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use vyuh::routes::IntoResponse;
use vyuh::{
    Error, SiteConf, Validate, bundles,
    errors::{ErrorConf, HttpErrorRenderMode},
    routes::{Form, Html, Json, Path, Query, StatusCode, Valid},
    testing::TestSite,
};

fn test_conf() -> SiteConf {
    SiteConf {
        log_init: false,
        logging: vyuh::logging::LoggingConf {
            env_prefix: None,
            rules: vec![],
        },
        ..SiteConf::default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
struct CreateUser {
    #[validate(email)]
    email: String,
    #[validate(min_length = 3)]
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
struct SearchUsers {
    #[validate(min_length = 2)]
    q: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
struct UserPath {
    #[validate(min = 1)]
    id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
struct CustomInput {
    #[validate(custom = "validate_slug", custom_schema = "slug")]
    visible_slug: String,
    #[validate(custom = "validate_slug")]
    hidden_slug: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
struct SchemaRules {
    #[validate(min = 1, exclusive_min, max = 10, exclusive_max, multiple_of = 3)]
    count: i32,
    #[validate(min_items = 1, max_items = 3, unique_items)]
    tags: Vec<String>,
    #[validate(enum_values("draft", "published"))]
    status: String,
    #[validate(phone_e164)]
    phone: String,
    #[validate(ipv6)]
    ip6: String,
    #[validate(date)]
    date: String,
    #[validate(datetime)]
    datetime: String,
}

fn validate_slug(value: &str) -> Result<(), vyuh::ValidationError> {
    if value.chars().all(|ch| ch.is_ascii_lowercase() || ch == '-') {
        Ok(())
    } else {
        Err(vyuh::ValidationError::new("slug", "Enter a valid slug."))
    }
}

#[bundles::route(path = "/parse", method = "POST")]
async fn parse_only(Json(input): Json<CreateUser>) -> Json<CreateUser> {
    Json(input)
}

#[bundles::route(path = "/valid", method = "POST")]
async fn valid_json(Valid(Json(input)): Valid<Json<CreateUser>>) -> Json<CreateUser> {
    Json(input)
}

#[bundles::route(path = "/valid-form", method = "POST")]
async fn valid_form(Valid(Form(input)): Valid<Form<CreateUser>>) -> Json<CreateUser> {
    Json(input)
}

#[bundles::route(path = "/search")]
async fn valid_query(Valid(Query(input)): Valid<Query<SearchUsers>>) -> Json<SearchUsers> {
    Json(input)
}

#[bundles::route(path = "/users/{id}")]
async fn valid_path(Valid(Path(input)): Valid<Path<UserPath>>) -> Json<UserPath> {
    Json(input)
}

#[bundles::route(path = "/custom", method = "POST")]
async fn valid_custom(Valid(Json(input)): Valid<Json<CustomInput>>) -> Json<CustomInput> {
    Json(input)
}

#[bundles::route(path = "/schema-rules", method = "POST")]
async fn valid_schema_rules(Valid(Json(input)): Valid<Json<SchemaRules>>) -> Json<SchemaRules> {
    Json(input)
}

#[bundles::route(path = "/bad-html")]
async fn bad_html() -> Result<Json<()>, Error> {
    Err(Error::bad_request("<script>alert(1)</script>"))
}

async fn validation_site(openapi: bool) -> vyuh::Site {
    let bundle = bundles::bundle! {
        parse_only,
        valid_json,
        valid_form,
        valid_query,
        valid_path,
        valid_custom,
        valid_schema_rules,
    };
    let bundle = if openapi {
        bundle.with_openapi(
            bundles::OpenApiConf::default()
                .title("Validation API")
                .version("0.1.0")
                .spec("/openapi.json")
                .public(),
        )
    } else {
        bundle
    };
    vyuh::Site::build(test_conf(), bundle).await.unwrap()
}

#[tokio::test]
async fn plain_json_does_not_validate() {
    let site = validation_site(false).await;
    let client = TestSite::new(site.clone());

    client
        .post("/parse")
        .json(&serde_json::json!({
            "email": "not-an-email",
            "name": "x"
        }))
        .send()
        .await
        .assert_status(StatusCode::OK);

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn valid_json_returns_structured_422() {
    let site = validation_site(false).await;
    let client = TestSite::new(site.clone());

    let body: Value = client
        .post("/valid")
        .json(&serde_json::json!({
            "email": "not-an-email",
            "name": "x"
        }))
        .send()
        .await
        .assert_status(StatusCode::UNPROCESSABLE_ENTITY)
        .json()
        .await;

    assert_eq!(body["source"], "validation");
    assert_eq!(body["code"], "validation_error");
    assert!(body["errors"]["email"].is_array());
    assert!(body["errors"]["name"].is_array());
    assert_eq!(body["errors"]["email"][0]["code"], "email");
    assert_eq!(
        body["errors"]["email"][0]["message"],
        "Enter a valid email address."
    );
    assert_eq!(body["errors"]["email"][0]["params"], serde_json::json!({}));
    assert_eq!(body["errors"]["name"][0]["code"], "min_length");
    assert_eq!(body["errors"]["name"][0]["params"]["min"], "3");

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn valid_query_and_path_share_error_shape() {
    let site = validation_site(false).await;
    let client = TestSite::new(site.clone());

    let query_body: Value = client
        .get("/search?q=x")
        .header("accept", "application/json")
        .send()
        .await
        .assert_status(StatusCode::UNPROCESSABLE_ENTITY)
        .json()
        .await;
    assert_eq!(query_body["source"], "validation");
    assert!(query_body["errors"]["q"].is_array());

    let path_body: Value = client
        .get("/users/0")
        .header("accept", "application/json")
        .send()
        .await
        .assert_status(StatusCode::UNPROCESSABLE_ENTITY)
        .json()
        .await;
    assert_eq!(path_body["source"], "validation");
    assert!(path_body["errors"]["id"].is_array());

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn parse_errors_return_400_error_report() {
    let site = validation_site(false).await;
    let client = TestSite::new(site.clone());

    let body: Value = client
        .post("/parse")
        .header("content-type", "application/json")
        .body(axum::body::Body::from("{bad json"))
        .send()
        .await
        .assert_status(StatusCode::BAD_REQUEST)
        .json()
        .await;

    assert_eq!(body["source"], "parse");
    assert_eq!(body["code"], "bad_request");

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn auto_error_rendering_uses_json_for_json_requests() {
    let site = validation_site(false).await;
    let client = TestSite::new(site.clone());

    let json_body: Value = client
        .post("/valid")
        .json(&serde_json::json!({
            "email": "not-an-email",
            "name": "x"
        }))
        .send()
        .await
        .assert_status(StatusCode::UNPROCESSABLE_ENTITY)
        .json()
        .await;
    assert_eq!(json_body["source"], "validation");

    let api_body: Value = client
        .post("/parse")
        .header("content-type", "application/vnd.api+json")
        .body(axum::body::Body::from("{bad json"))
        .send()
        .await
        .assert_status(StatusCode::BAD_REQUEST)
        .json()
        .await;
    assert_eq!(api_body["code"], "bad_request");

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn auto_error_rendering_uses_json_for_json_accept() {
    let site = validation_site(false).await;
    let client = TestSite::new(site.clone());

    let body: Value = client
        .get("/search?q=x")
        .header("accept", "application/vnd.api+json")
        .send()
        .await
        .assert_status(StatusCode::UNPROCESSABLE_ENTITY)
        .json()
        .await;

    assert_eq!(body["code"], "validation_error");
    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn auto_error_rendering_uses_html_without_json_signal() {
    let site = validation_site(false).await;
    let client = TestSite::new(site.clone());

    let response = client
        .post("/valid-form")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(axum::body::Body::from("email=bad&name=x"))
        .send()
        .await
        .assert_status(StatusCode::UNPROCESSABLE_ENTITY);
    let content_type = response
        .header("content-type")
        .and_then(|value| value.to_str().ok());
    assert!(content_type.is_some_and(|value| value.starts_with("text/html")));
    let html = response.text().await;
    assert!(html.contains("validation_error"));

    let wildcard = client
        .get("/search?q=x")
        .header("accept", "*/*")
        .send()
        .await
        .assert_status(StatusCode::UNPROCESSABLE_ENTITY)
        .text()
        .await;
    assert!(wildcard.contains("<!doctype html>"));

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn auto_error_rendering_escapes_default_html() {
    let site = vyuh::Site::build(
        test_conf(),
        bundles::bundle! {
            bad_html,
        },
    )
    .await
    .unwrap();
    let client = TestSite::new(site.clone());

    let html = client
        .get("/bad-html")
        .send()
        .await
        .assert_status(StatusCode::BAD_REQUEST)
        .text()
        .await;

    assert!(html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
    assert!(!html.contains("<script>"));
    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn openapi_constraints_appear_only_for_valid_inputs() {
    let site = validation_site(true).await;
    let client = TestSite::new(site.clone());

    let spec: Value = client
        .get("/openapi.json")
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;

    let components = &spec["components"]["schemas"];
    let create_user = components
        .as_object()
        .and_then(|schemas| schemas.get("CreateUser"))
        .expect("CreateUser schema should exist");
    let plain_props = &create_user["properties"];
    assert!(plain_props["email"].get("format").is_none());
    assert!(plain_props["name"].get("minLength").is_none());

    let parse_body =
        &spec["paths"]["/parse"]["post"]["requestBody"]["content"]["application/json"]["schema"];
    let valid_body =
        &spec["paths"]["/valid"]["post"]["requestBody"]["content"]["application/json"]["schema"];

    assert_eq!(parse_body["$ref"], "#/components/schemas/CreateUser");
    assert!(valid_body.get("$ref").is_none());
    assert_eq!(valid_body["properties"]["email"]["format"], "email");
    assert_eq!(valid_body["properties"]["name"]["minLength"], 3);

    let custom_body =
        &spec["paths"]["/custom"]["post"]["requestBody"]["content"]["application/json"]["schema"];
    assert_eq!(
        custom_body["properties"]["visible_slug"]["x-vyuh-validators"][0],
        "slug"
    );
    assert!(
        custom_body["properties"]["hidden_slug"]
            .get("x-vyuh-validators")
            .is_none()
    );

    let schema_rules = &spec["paths"]["/schema-rules"]["post"]["requestBody"]["content"]["application/json"]
        ["schema"];
    assert_eq!(schema_rules["properties"]["count"]["minimum"], 1);
    assert_eq!(
        schema_rules["properties"]["count"]["exclusiveMinimum"],
        true
    );
    assert_eq!(schema_rules["properties"]["count"]["maximum"], 10);
    assert_eq!(
        schema_rules["properties"]["count"]["exclusiveMaximum"],
        true
    );
    assert_eq!(schema_rules["properties"]["count"]["multipleOf"], 3);
    assert_eq!(schema_rules["properties"]["tags"]["minItems"], 1);
    assert_eq!(schema_rules["properties"]["tags"]["maxItems"], 3);
    assert_eq!(schema_rules["properties"]["tags"]["uniqueItems"], true);
    assert_eq!(schema_rules["properties"]["status"]["enum"][0], "draft");
    assert_eq!(
        schema_rules["properties"]["phone"]["pattern"],
        r"^\+[1-9]\d{1,14}$"
    );
    assert_eq!(schema_rules["properties"]["ip6"]["format"], "ipv6");
    assert_eq!(schema_rules["properties"]["date"]["format"], "date");
    assert_eq!(
        schema_rules["properties"]["datetime"]["format"],
        "date-time"
    );

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn custom_error_handler_can_replace_response() {
    let conf = test_conf().errors(ErrorConf::default().handler(|ctx, report| async move {
        assert_eq!(ctx.path, "/valid");
        let body = format!("{:?}:{}", report.source, report.code);
        (
            StatusCode::IM_A_TEAPOT,
            [("content-type", "text/plain")],
            body,
        )
            .into_response()
    }));
    let site = vyuh::Site::build(
        conf,
        bundles::bundle! {
            valid_json,
            valid_form,
        },
    )
    .await
    .unwrap();
    let client = TestSite::new(site.clone());

    let text = client
        .post("/valid")
        .json(&serde_json::json!({
            "email": "not-an-email",
            "name": "x"
        }))
        .send()
        .await
        .assert_status(StatusCode::IM_A_TEAPOT)
        .text()
        .await;

    assert!(text.ends_with(":validation_error"));
    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn custom_json_and_html_error_renderers_can_replace_messages() {
    let conf = test_conf().errors(
        ErrorConf::default()
            .json(|ctx, view| async move {
                assert_eq!(ctx.path, "/valid");
                (
                    view.status,
                    Json(serde_json::json!({
                        "code": view.code,
                        "message": "json validation message",
                        "has_errors": view.errors.is_some(),
                    })),
                )
                    .into_response()
            })
            .html(|ctx, view| async move {
                assert_eq!(ctx.path, "/valid-form");
                (
                    view.status,
                    Html(format!(
                        "<h1>{}</h1><p>html validation message</p>",
                        view.code
                    )),
                )
                    .into_response()
            }),
    );
    let site = vyuh::Site::build(
        conf,
        bundles::bundle! {
            valid_json,
            valid_form,
        },
    )
    .await
    .unwrap();
    let client = TestSite::new(site.clone());

    let json_body: Value = client
        .post("/valid")
        .json(&serde_json::json!({
            "email": "not-an-email",
            "name": "x"
        }))
        .send()
        .await
        .assert_status(StatusCode::UNPROCESSABLE_ENTITY)
        .json()
        .await;
    assert_eq!(json_body["message"], "json validation message");
    assert_eq!(json_body["has_errors"], true);

    let json_wins: Value = client
        .post("/valid")
        .header("accept", "text/html")
        .json(&serde_json::json!({
            "email": "not-an-email",
            "name": "x"
        }))
        .send()
        .await
        .assert_status(StatusCode::UNPROCESSABLE_ENTITY)
        .json()
        .await;
    assert_eq!(json_wins["message"], "json validation message");

    let html_body = client
        .post("/valid-form")
        .header("accept", "text/html")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(axum::body::Body::from("email=bad&name=x"))
        .send()
        .await
        .assert_status(StatusCode::UNPROCESSABLE_ENTITY)
        .text()
        .await;
    assert!(html_body.contains("html validation message"));

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn html_error_renderer_can_be_forced_by_config() {
    let conf = test_conf().errors(
        ErrorConf::default()
            .http_mode(HttpErrorRenderMode::Html)
            .html(|_, view| async move {
                (view.status, Html(format!("html:{}", view.code))).into_response()
            }),
    );
    let site = vyuh::Site::build(
        conf,
        bundles::bundle! {
            valid_json,
        },
    )
    .await
    .unwrap();
    let client = TestSite::new(site.clone());

    let html_body = client
        .post("/valid")
        .json(&serde_json::json!({
            "email": "not-an-email",
            "name": "x"
        }))
        .send()
        .await
        .assert_status(StatusCode::UNPROCESSABLE_ENTITY)
        .text()
        .await;
    assert_eq!(html_body, "html:validation_error");

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn json_error_renderer_can_be_forced_by_config() {
    let conf = test_conf().errors(ErrorConf::default().http_mode(HttpErrorRenderMode::Json));
    let site = vyuh::Site::build(
        conf,
        bundles::bundle! {
            valid_form,
        },
    )
    .await
    .unwrap();
    let client = TestSite::new(site.clone());

    let body: Value = client
        .post("/valid-form")
        .header("accept", "text/html")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(axum::body::Body::from("email=bad&name=x"))
        .send()
        .await
        .assert_status(StatusCode::UNPROCESSABLE_ENTITY)
        .json()
        .await;
    assert_eq!(body["code"], "validation_error");

    site.shutdown_and_wait().await;
}
