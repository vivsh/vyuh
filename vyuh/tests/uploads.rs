use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use vyuh::callables::{MultipartApiFieldKind, MultipartApiSpec};
use vyuh::file_storage::{StorageName, UploadConf};
use vyuh::routes::multipart::{FieldRule, FileRule, MultipartMap, MultipartSpec, UploadedFile};
use vyuh::routes::{Json, MultipartForm, StatusCode};
use vyuh::{Data, Error, Site, SiteConf, Validate, bundles};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct UploadOut {
    name: String,
    size: u64,
    sniffed: Option<String>,
}

#[derive(Debug, Clone, JsonSchema, vyuh::MultipartData)]
struct AvatarUpload {
    display_name: String,
    #[upload(
        content_types = ["image/png"],
        extensions = ["png"],
        sniff = "image",
        max_size = 64
    )]
    avatar: UploadedFile,
}

#[derive(Debug, Clone, JsonSchema, vyuh::MultipartData)]
struct OptionalUpload {
    title: String,
    published: bool,
    post_id: Option<i64>,
    #[upload(
        content_types = ["image/png"],
        extensions = ["png"],
        sniff = "image",
        max_size = 64
    )]
    image: Option<UploadedFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct OptionalOut {
    title: String,
    published: bool,
    post_id: Option<i64>,
    image_size: Option<u64>,
    sniffed: Option<String>,
}

#[derive(Debug, Clone, JsonSchema, vyuh::MultipartData)]
struct GalleryUpload {
    title: String,
    #[upload(
        content_types = ["image/png"],
        extensions = ["png"],
        sniff = "image",
        max_size = 64
    )]
    images: Vec<UploadedFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct GalleryOut {
    title: String,
    image_count: usize,
    total_size: u64,
}

impl Validate for AvatarUpload {
    fn validate(&self) -> Result<(), vyuh::ValidationReport> {
        Ok(())
    }
}

impl vyuh::ValidationSchema for AvatarUpload {
    fn apply_validation_schema(
        _schema: &mut serde_json::Value,
        _definitions: &mut serde_json::Map<String, serde_json::Value>,
    ) {
    }
}

/// Verifies multipart rule metadata can be converted into OpenAPI-safe upload metadata.
#[test]
fn multipart_spec_converts_to_api_metadata() {
    let spec = MultipartSpec::new()
        .text(
            "display_name",
            FieldRule::new()
                .required()
                .max_length(80)
                .max_bytes(120)
                .multiple(),
        )
        .file(
            "avatar",
            FileRule::new()
                .required()
                .content_types(["image/png", "image/jpeg"])
                .extensions(["png", "jpg"])
                .sniff_image()
                .max_size(2_000_000),
        )
        .file("gallery", FileRule::new().multiple())
        .allow_unknown(true);

    let api = MultipartApiSpec::from(&spec);

    assert!(api.allow_unknown);
    let display = api
        .fields
        .iter()
        .find(|field| field.name == "display_name")
        .unwrap();
    assert_eq!(display.kind, MultipartApiFieldKind::Text);
    assert!(display.required);
    assert!(display.multiple);
    assert_eq!(display.max_length, Some(80));
    assert_eq!(display.max_bytes, Some(120));

    let avatar = api
        .fields
        .iter()
        .find(|field| field.name == "avatar")
        .unwrap();
    assert_eq!(avatar.kind, MultipartApiFieldKind::File);
    assert!(avatar.required);
    assert_eq!(avatar.max_bytes, Some(2_000_000));
    assert_eq!(avatar.content_types, vec!["image/jpeg", "image/png"]);
    assert_eq!(avatar.extensions, vec!["jpg", "png"]);
    assert_eq!(avatar.sniff.as_deref(), Some("image"));

    let gallery = api
        .fields
        .iter()
        .find(|field| field.name == "gallery")
        .unwrap();
    assert!(gallery.multiple);
}

#[bundles::route(path = "/typed", method = "POST")]
async fn typed_upload(MultipartForm(input): MultipartForm<AvatarUpload>) -> Json<UploadOut> {
    Json(UploadOut {
        name: input.display_name,
        size: input.avatar.size(),
        sniffed: input.avatar.sniffed_content_type().map(ToOwned::to_owned),
    })
}

#[bundles::route(path = "/optional", method = "POST")]
async fn optional_upload(MultipartForm(input): MultipartForm<OptionalUpload>) -> Json<OptionalOut> {
    Json(OptionalOut {
        title: input.title,
        published: input.published,
        post_id: input.post_id,
        image_size: input.image.as_ref().map(UploadedFile::size),
        sniffed: input
            .image
            .as_ref()
            .and_then(|image| image.sniffed_content_type().map(ToOwned::to_owned)),
    })
}

#[bundles::route(path = "/gallery", method = "POST")]
async fn gallery_upload(MultipartForm(input): MultipartForm<GalleryUpload>) -> Json<GalleryOut> {
    Json(GalleryOut {
        title: input.title,
        image_count: input.images.len(),
        total_size: input.images.iter().map(UploadedFile::size).sum(),
    })
}

#[bundles::route(path = "/macro-less", method = "POST")]
async fn macro_less_upload(site: Site, form: MultipartMap) -> Result<Data<UploadOut>, Error> {
    let form = form.validate(
        MultipartSpec::new()
            .text("display_name", FieldRule::new().required().max_length(80))
            .file(
                "avatar",
                FileRule::new()
                    .required()
                    .content_types(["image/png"])
                    .extensions(["png"])
                    .sniff_image()
                    .max_size(64),
            ),
    )?;
    let avatar = form.file("avatar")?;
    let saved = site.file_storage().save(avatar).await?;
    Ok(Data::new(UploadOut {
        name: saved.name.to_string(),
        size: avatar.size(),
        sniffed: avatar.sniffed_content_type().map(ToOwned::to_owned),
    }))
}

fn multipart_body(boundary: &str, file_name: &str, content_type: &str, file: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"display_name\"\r\n\r\nViv\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"avatar\"; filename=\"{file_name}\"\r\nContent-Type: {content_type}\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(file);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body
}

fn optional_body(boundary: &str, image: Option<(&str, &str, &[u8])>) -> Vec<u8> {
    let mut body = Vec::new();
    text_part(&mut body, boundary, "title", "Draft");
    text_part(&mut body, boundary, "published", "true");
    text_part(&mut body, boundary, "post_id", "42");
    if let Some((file_name, content_type, file)) = image {
        file_part(&mut body, boundary, "image", file_name, content_type, file);
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

fn gallery_body(boundary: &str, images: &[(&str, &str, &[u8])]) -> Vec<u8> {
    let mut body = Vec::new();
    text_part(&mut body, boundary, "title", "Gallery");
    for (file_name, content_type, file) in images {
        file_part(&mut body, boundary, "images", file_name, content_type, file);
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

fn text_part(body: &mut Vec<u8>, boundary: &str, name: &str, value: &str) {
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
        )
        .as_bytes(),
    );
}

fn file_part(
    body: &mut Vec<u8>,
    boundary: &str,
    name: &str,
    file_name: &str,
    content_type: &str,
    file: &[u8],
) {
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"; filename=\"{file_name}\"\r\nContent-Type: {content_type}\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(file);
    body.extend_from_slice(b"\r\n");
}

fn png_bytes() -> Vec<u8> {
    let mut bytes = vec![0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n'];
    bytes.extend_from_slice(&[0; 16]);
    bytes
}

async fn upload_site() -> (vyuh::Site, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let conf = SiteConf {
        log_init: false,
        project_dir: dir.path().to_string_lossy().to_string(),
        uploads: UploadConf {
            dir: "uploads".into(),
            temp_dir: Some("tmp".into()),
            memory_threshold_bytes: 8,
            max_request_bytes: 512,
            max_file_bytes: 128,
            ..UploadConf::default()
        },
        logging: vyuh::logging::LoggingConf {
            env_prefix: None,
            rules: vec![],
        },
        ..SiteConf::default()
    };
    let site = vyuh::Site::build(
        conf,
        bundles::bundle! {
            typed_upload,
            optional_upload,
            gallery_upload,
            macro_less_upload,
        },
    )
    .await
    .unwrap();
    (site, dir)
}

#[tokio::test]
async fn optional_file_may_be_absent() {
    let (site, _dir) = upload_site().await;
    let client = vyuh::testing::TestClient::new(site.clone());
    let boundary = "vyuh-boundary";
    let body = optional_body(boundary, None);

    let out: OptionalOut = client
        .post("/optional")
        .header(
            "content-type",
            &format!("multipart/form-data; boundary={boundary}"),
        )
        .body(axum::body::Body::from(body))
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;

    assert_eq!(out.title, "Draft");
    assert!(out.published);
    assert_eq!(out.post_id, Some(42));
    assert_eq!(out.image_size, None);
    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn optional_empty_file_field_is_absent() {
    let (site, _dir) = upload_site().await;
    let client = vyuh::testing::TestClient::new(site.clone());
    let boundary = "vyuh-boundary";
    let body = optional_body(boundary, Some(("", "application/octet-stream", &[])));

    let out: OptionalOut = client
        .post("/optional")
        .header(
            "content-type",
            &format!("multipart/form-data; boundary={boundary}"),
        )
        .body(axum::body::Body::from(body))
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;

    assert_eq!(out.image_size, None);
    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn vec_file_field_collects_repeated_files() {
    let (site, _dir) = upload_site().await;
    let client = vyuh::testing::TestClient::new(site.clone());
    let boundary = "vyuh-boundary";
    let first = png_bytes();
    let second = png_bytes();
    let body = gallery_body(
        boundary,
        &[
            ("first.png", "image/png", &first),
            ("second.png", "image/png", &second),
        ],
    );

    let out: GalleryOut = client
        .post("/gallery")
        .header(
            "content-type",
            &format!("multipart/form-data; boundary={boundary}"),
        )
        .body(axum::body::Body::from(body))
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;

    assert_eq!(out.title, "Gallery");
    assert_eq!(out.image_count, 2);
    assert_eq!(out.total_size, (first.len() + second.len()) as u64);
    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn optional_file_accepts_real_image() {
    let (site, _dir) = upload_site().await;
    let client = vyuh::testing::TestClient::new(site.clone());
    let boundary = "vyuh-boundary";
    let image = png_bytes();
    let body = optional_body(boundary, Some(("image.png", "image/png", &image)));

    let out: OptionalOut = client
        .post("/optional")
        .header(
            "content-type",
            &format!("multipart/form-data; boundary={boundary}"),
        )
        .body(axum::body::Body::from(body))
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;

    assert_eq!(out.image_size, Some(image.len() as u64));
    assert_eq!(out.sniffed.as_deref(), Some("image/png"));
    site.shutdown_and_wait().await;
}

async fn upload_openapi_site() -> (vyuh::Site, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let conf = SiteConf {
        log_init: false,
        project_dir: dir.path().to_string_lossy().to_string(),
        logging: vyuh::logging::LoggingConf {
            env_prefix: None,
            rules: vec![],
        },
        ..SiteConf::default()
    };
    let bundle = bundles::bundle! {
        typed_upload,
    }
    .with_openapi(
        bundles::OpenApiConf::default()
            .title("Uploads")
            .version("0.1.0")
            .spec("/openapi.json"),
    );
    let site = vyuh::Site::build(conf, bundle).await.unwrap();
    (site, dir)
}

#[tokio::test]
async fn typed_multipart_accepts_sniffed_png() {
    let (site, _dir) = upload_site().await;
    let client = vyuh::testing::TestClient::new(site.clone());
    let boundary = "vyuh-boundary";
    let body = multipart_body(boundary, "avatar.png", "image/png", &png_bytes());

    let out: UploadOut = client
        .post("/typed")
        .header("accept", "application/json")
        .header(
            "content-type",
            &format!("multipart/form-data; boundary={boundary}"),
        )
        .body(axum::body::Body::from(body))
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;

    assert_eq!(out.name, "Viv");
    assert_eq!(out.sniffed.as_deref(), Some("image/png"));
    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn macro_less_upload_saves_file_with_local_storage() {
    let (site, dir) = upload_site().await;
    let client = vyuh::testing::TestClient::new(site.clone());
    let boundary = "vyuh-boundary";
    let body = multipart_body(boundary, "avatar.png", "image/png", &png_bytes());

    let out: UploadOut = client
        .post("/macro-less")
        .header(
            "content-type",
            &format!("multipart/form-data; boundary={boundary}"),
        )
        .body(axum::body::Body::from(body))
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;

    assert!(dir.path().join("uploads").join(&out.name).exists());
    assert_eq!(out.size, png_bytes().len() as u64);
    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn invalid_sniffed_file_is_rejected() {
    let (site, _dir) = upload_site().await;
    let client = vyuh::testing::TestClient::new(site.clone());
    let boundary = "vyuh-boundary";
    let body = multipart_body(boundary, "avatar.png", "image/png", b"not an image");

    let body: Value = client
        .post("/typed")
        .header("accept", "application/json")
        .header(
            "content-type",
            &format!("multipart/form-data; boundary={boundary}"),
        )
        .body(axum::body::Body::from(body))
        .send()
        .await
        .assert_status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
        .json()
        .await;

    assert_eq!(body["code"], "unsupported_upload");
    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn multipart_errors_render_html_by_default() {
    let (site, _dir) = upload_site().await;
    let client = vyuh::testing::TestClient::new(site.clone());
    let boundary = "vyuh-boundary";
    let body = multipart_body(boundary, "avatar.png", "image/png", b"not an image");

    let response = client
        .post("/typed")
        .header(
            "content-type",
            &format!("multipart/form-data; boundary={boundary}"),
        )
        .body(axum::body::Body::from(body))
        .send()
        .await
        .assert_status(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let content_type = response
        .header("content-type")
        .and_then(|value| value.to_str().ok());
    assert!(content_type.is_some_and(|value| value.starts_with("text/html")));
    let html = response.text().await;
    assert!(html.contains("unsupported_upload"));

    site.shutdown_and_wait().await;
}

#[tokio::test]
async fn oversized_file_is_rejected() {
    let (site, _dir) = upload_site().await;
    let client = vyuh::testing::TestClient::new(site.clone());
    let boundary = "vyuh-boundary";
    let body = multipart_body(boundary, "avatar.png", "image/png", &[0x89; 80]);

    client
        .post("/typed")
        .header(
            "content-type",
            &format!("multipart/form-data; boundary={boundary}"),
        )
        .body(axum::body::Body::from(body))
        .send()
        .await
        .assert_status(StatusCode::PAYLOAD_TOO_LARGE);

    site.shutdown_and_wait().await;
}

#[test]
fn unsafe_storage_names_are_rejected() {
    assert!(StorageName::new("../avatar.png").is_err());
    assert!(StorageName::new("/avatar.png").is_err());
    assert!(StorageName::new("nested\\avatar.png").is_err());
    assert!(StorageName::new("avatar.png").is_ok());
}

#[tokio::test]
async fn multipart_openapi_documents_binary_file_field() {
    let (site, _dir) = upload_openapi_site().await;
    let client = vyuh::testing::TestClient::new(site.clone());

    let spec: Value = client
        .get("/openapi.json")
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;

    let schema =
        &spec["paths"]["/typed"]["post"]["requestBody"]["content"]["multipart/form-data"]["schema"];
    assert_eq!(schema["$ref"], "#/components/schemas/AvatarUpload");
    let avatar = &spec["components"]["schemas"]["AvatarUpload"]["properties"]["avatar"];
    assert_eq!(avatar["type"], "string");
    assert_eq!(avatar["format"], "binary");

    site.shutdown_and_wait().await;
}
