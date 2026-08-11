use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use vyuh::{
    Data, SiteConf, SiteConfig, Validate,
    auth::{Audience, AuthConf, AuthUser, DEFAULT_AUTH_PROVIDER},
    bundles,
    routes::{BodyBytes, Json, Path, Query, StatusCode, Valid},
    testing::TestSite,
};

fn test_conf() -> SiteConf {
    SiteConf {
        log_init: false,
        logging: vyuh::logging::LoggingConf {
            env_prefix: None,
            rules: vec![],
        },
        auth: AuthConf::development(),
        ..SiteConf::default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
struct CreateNote {
    #[validate(min_length = 3)]
    title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct SearchParams {
    q: String,
    page: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct UserPath {
    id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct NoteOut {
    id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct UserInOrg {
    org: String,
    id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct WebhookResult {
    len: usize,
}

#[bundles::route(path = "/notes", method = "POST")]
async fn create_note(Json(input): Json<CreateNote>) -> Json<CreateNote> {
    Json(input)
}

#[bundles::route(path = "/validated-notes", method = "POST")]
async fn create_valid_note(Valid(Json(input)): Valid<Json<CreateNote>>) -> Json<CreateNote> {
    Json(input)
}

#[bundles::route(path = "/data-notes", method = "POST")]
async fn create_data_note(Data(input): Data<CreateNote>) -> Data<CreateNote> {
    Data::from_arc(input)
}

#[bundles::route(path = "/validated-data-notes", method = "POST")]
async fn create_valid_data_note(Valid(Data(input)): Valid<Data<CreateNote>>) -> Data<CreateNote> {
    Data::from_arc(input)
}

#[bundles::route(path = "/site-config")]
async fn site_config(config: SiteConfig) -> Json<String> {
    Json(config.host.clone())
}

#[bundles::route(path = "/search")]
async fn search(Query(params): Query<SearchParams>) -> Json<SearchParams> {
    Json(params)
}

#[bundles::route(path = "/users/{id}")]
async fn user_detail(Path(path): Path<UserPath>) -> Json<NoteOut> {
    Json(NoteOut { id: path.id })
}

#[bundles::route(path = "/orgs/{org}/users/{id}")]
async fn user_in_org(Path((org, id)): Path<(String, u64)>) -> Json<UserInOrg> {
    Json(UserInOrg { org, id })
}

#[bundles::route(path = "/webhook", method = "POST")]
async fn webhook(BodyBytes(bytes): BodyBytes) -> Json<WebhookResult> {
    Json(WebhookResult { len: bytes.len() })
}

async fn request_site(openapi: bool) -> vyuh::Site {
    let bundle = bundles::bundle! {
        create_note,
        create_valid_note,
        create_data_note,
        create_valid_data_note,
        site_config,
        search,
        user_detail,
        user_in_org,
        webhook,
    };
    let bundle = if openapi {
        bundle.with_conf(
            bundles::conf().openapi(
                bundles::OpenApiConf::default()
                    .title("Request")
                    .version("0.1.0")
                    .spec("/openapi.json")
                    .public(),
            ),
        )
    } else {
        bundle
    };
    vyuh::Site::build(test_conf(), bundle).await.unwrap()
}

fn docs_admin(user: &AuthUser) -> bool {
    user.subject() == "docs-admin"
}

const DOCS: Audience = Audience::new("docs");

async fn openapi_access_site(conf: bundles::OpenApiConf) -> Result<vyuh::Site, String> {
    let bundle = bundles::bundle! { create_note }.with_conf(bundles::conf().openapi(conf));
    vyuh::Site::build(test_conf(), bundle)
        .await
        .map_err(|error| error.to_string())
}

async fn audience_openapi_site(conf: bundles::OpenApiConf) -> Result<vyuh::Site, String> {
    let bundle =
        bundles::bundle! { create_note }.with_conf(bundles::conf().audience(DOCS).openapi(conf));
    vyuh::Site::build(test_conf(), bundle)
        .await
        .map_err(|error| error.to_string())
}

/// Verifies OpenAPI is private by default while allowing explicit public and restricted access.
#[tokio::test]
async fn openapi_access_is_private_by_default() -> Result<(), String> {
    let site = openapi_access_site(
        bundles::OpenApiConf::default()
            .spec("/openapi.json")
            .viewer("/docs"),
    )
    .await?;
    let client = TestSite::new(site.clone());

    client
        .get("/openapi.json")
        .send()
        .await
        .assert_status(StatusCode::UNAUTHORIZED);
    client
        .get("/docs")
        .send()
        .await
        .assert_status(StatusCode::UNAUTHORIZED);

    let login = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .issue(AuthUser::new("reader"), &[])
        .await
        .map_err(|error| error.to_string())?;
    client
        .get("/openapi.json")
        .with_login(&login)
        .send()
        .await
        .assert_status(StatusCode::OK);
    client
        .get("/docs")
        .with_login(&login)
        .send()
        .await
        .assert_status(StatusCode::OK);
    site.shutdown_and_wait().await;

    let restricted = openapi_access_site(
        bundles::OpenApiConf::default()
            .spec("/restricted.json")
            .auth(docs_admin),
    )
    .await?;
    let denied_login = restricted
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .issue(AuthUser::new("reader"), &[])
        .await
        .map_err(|error| error.to_string())?;
    TestSite::new(restricted.clone())
        .get("/restricted.json")
        .with_login(&denied_login)
        .send()
        .await
        .assert_status(StatusCode::FORBIDDEN);
    restricted.shutdown_and_wait().await;

    let audience_site =
        audience_openapi_site(bundles::OpenApiConf::default().spec("/docs.json")).await?;
    let default_login = audience_site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .issue(AuthUser::new("reader"), &[])
        .await
        .map_err(|error| error.to_string())?;
    let docs_login = audience_site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .issue(AuthUser::new("reader"), &[DOCS])
        .await
        .map_err(|error| error.to_string())?;
    let audience_client = TestSite::new(audience_site.clone());
    audience_client
        .get("/docs.json")
        .with_login(&default_login)
        .send()
        .await
        .assert_status(StatusCode::UNAUTHORIZED);
    audience_client
        .get("/docs.json")
        .with_login(&docs_login)
        .send()
        .await
        .assert_status(StatusCode::OK);
    audience_site.shutdown_and_wait().await;

    let public = openapi_access_site(
        bundles::OpenApiConf::default()
            .spec("/public.json")
            .public(),
    )
    .await?;
    TestSite::new(public.clone())
        .get("/public.json")
        .send()
        .await
        .assert_status(StatusCode::OK);
    public.shutdown_and_wait().await;
    Ok(())
}

#[tokio::test]
async fn request_documentation_signatures_work() {
    let site = request_site(false).await;
    let client = TestSite::new(site.clone());

    let tuple_path: Value = client
        .get("/orgs/acme/users/42")
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;
    assert_eq!(tuple_path["org"], "acme");
    assert_eq!(tuple_path["id"], 42);

    let webhook: Value = client
        .post("/webhook")
        .body(axum::body::Body::from("signed-payload"))
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;
    assert_eq!(webhook["len"], 14);

    let parse_only: Value = client
        .post("/notes")
        .json(&serde_json::json!({ "title": "x" }))
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;
    assert_eq!(parse_only["title"], "x");

    let data_parse_only: Value = client
        .post("/data-notes")
        .json(&serde_json::json!({ "title": "x" }))
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;
    assert_eq!(data_parse_only["title"], "x");

    client
        .post("/validated-notes")
        .json(&serde_json::json!({ "title": "x" }))
        .send()
        .await
        .assert_status(StatusCode::UNPROCESSABLE_ENTITY);

    client
        .post("/validated-data-notes")
        .json(&serde_json::json!({ "title": "x" }))
        .send()
        .await
        .assert_status(StatusCode::UNPROCESSABLE_ENTITY);

    let config_host: Value = client
        .get("/site-config")
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;
    assert_eq!(config_host, "localhost");

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn body_bytes_is_documented_as_binary_openapi_body() {
    let site = request_site(true).await;
    let client = TestSite::new(site.clone());

    let spec: Value = client
        .get("/openapi.json")
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;

    let schema = &spec["paths"]["/webhook"]["post"]["requestBody"]["content"]["application/octet-stream"]
        ["schema"];
    assert_eq!(schema["type"], "string");
    assert_eq!(schema["format"], "binary");

    let plain_data_schema = &spec["paths"]["/data-notes"]["post"]["requestBody"]["content"]["application/json"]
        ["schema"];
    let validated_data_schema = &spec["paths"]["/validated-data-notes"]["post"]["requestBody"]["content"]
        ["application/json"]["schema"];
    assert!(plain_data_schema.to_string().contains("CreateNote"));
    assert_eq!(validated_data_schema["properties"]["title"]["minLength"], 3);

    site.shutdown_and_wait().await;
}
