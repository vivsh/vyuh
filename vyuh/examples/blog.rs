//! Postgres blog example.
//!
//! Set the database URL and a development secret before running commands:
//!
//! ```sh
//! export DATABASE_URL=postgres://postgres:postgres@localhost/vyuh_blog
//! export VYUH_SECRET_KEY=replace-with-at-least-32-random-characters
//! ```
//!
//! Apply migrations:
//!
//! ```sh
//! cargo run -p vyuh --features postgres,migrations --example blog -- migrate
//! ```
//!
//! Create an admin user:
//!
//! ```sh
//! cargo run -p vyuh --features postgres,migrations --example blog -- users:create-admin --username admin --display-name Admin --password change-me
//! ```
//!
//! Start the server:
//!
//! ```sh
//! cargo run -p vyuh --features postgres,migrations --example blog -- serve
//! ```
//!
//! Open http://127.0.0.1:8080/ and sign in with the admin username and password
//! created above.
//!
//! Other useful pages:
//! - http://127.0.0.1:8080/console
//! - http://127.0.0.1:8080/docs

use std::convert::Infallible;

use axum::{body::Body, extract::FromRequestParts, http::request::Parts, response::Response};
use serde_json::json;
use tokio_util::io::ReaderStream;
use vyuh::{
    ErrorKind,
    auth::{AuthConf, AuthUser, BitRole, check_password, make_password},
    commands::CommandConf,
    console::ConsoleConf,
    errors::{ErrorConf, HttpErrorRenderMode},
    file_storage::StorageName,
    prelude::*,
    routes::{FileResponse, MultipartForm, UploadedFile},
};

static MIGRATIONS: db::EmbeddedMigrations = db::embedded_migrations!("examples/blog/migrations");
const DEFAULT_PAGE_SIZE: usize = 9;
const MAX_PAGE_SIZE: usize = 50;
const ADMIN_POST_PAGE_SIZE: usize = 8;

#[derive(BitRole)]
enum UserRole {
    User,
    Admin,
}

type BlogUser = vyuh::permit!(UserRole, User);
type AdminUser = vyuh::permit!(UserRole, Admin);

#[derive(Debug, Clone, Serialize, Deserialize, db::Model)]
#[table(name = "blog_users")]
struct User {
    #[column(primary_key, serial)]
    id: i64,
    #[column(unique)]
    username: String,
    display_name: String,
    password_hash: String,
    #[column(default = "false")]
    is_admin: bool,
    #[column(type = "timestamp with time zone", default = "now()")]
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, db::Model)]
#[table(name = "blog_posts")]
struct Post {
    #[column(primary_key, serial)]
    id: i64,
    title: String,
    #[column(unique)]
    slug: String,
    excerpt: String,
    body: String,
    image_url: Option<String>,
    #[column(
        references = "blog_users.id",
        references_name = "blog_posts_author_id_fkey",
        index
    )]
    author_id: i64,
    #[column(default = "false")]
    published: bool,
    #[column(type = "timestamp with time zone", default = "now()")]
    created_at: chrono::DateTime<chrono::Utc>,
    #[column(type = "timestamp with time zone", default = "now()")]
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, db::Model)]
#[table(name = "blog_comments")]
struct Comment {
    #[column(primary_key, serial)]
    id: i64,
    #[column(
        references = "blog_posts.id",
        references_name = "blog_comments_post_id_fkey",
        index
    )]
    post_id: i64,
    #[column(
        references = "blog_users.id",
        references_name = "blog_comments_author_id_fkey",
        index
    )]
    author_id: i64,
    body: String,
    #[column(type = "timestamp with time zone", default = "now()")]
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, db::Record)]
struct PostWithAuthor {
    #[column(flatten)]
    post: Post,
    #[column(reference(from = "author_id", to = "id"))]
    author: User,
}

#[derive(Debug, Clone, Serialize, Deserialize, db::Record)]
struct CommentWithAuthor {
    #[column(flatten)]
    comment: Comment,
    #[column(reference(from = "author_id", to = "id"))]
    author: User,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
struct LoginInput {
    #[validate(min_length = 1)]
    username: String,
    #[validate(min_length = 1)]
    password: String,
}

#[derive(Debug, Clone, JsonSchema, Validate, vyuh::MultipartData)]
struct PostForm {
    #[validate(min_length = 3, max_length = 120)]
    title: String,
    #[validate(min_length = 1, max_length = 240)]
    excerpt: String,
    #[validate(min_length = 1)]
    body: String,
    published: bool,
    #[upload(
        content_types = ["image/png", "image/jpeg", "image/webp"],
        extensions = ["png", "jpg", "jpeg", "webp"],
        sniff = "image",
        max_size = 3_000_000
    )]
    image: Option<UploadedFile>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
struct CommentInput {
    #[validate(min_length = 1, max_length = 2_000)]
    body: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
struct UserInput {
    #[validate(min_length = 3, max_length = 80)]
    username: String,
    #[validate(min_length = 1, max_length = 120)]
    display_name: String,
    #[validate(min_length = 8)]
    password: String,
    #[serde(default)]
    is_admin: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Default)]
struct PostsQuery {
    #[serde(default)]
    all: bool,
    #[serde(default)]
    page: Option<usize>,
    #[serde(default)]
    per_page: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Default)]
struct PageQuery {
    #[serde(default)]
    page: Option<usize>,
    #[serde(default)]
    per_page: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
struct StatusInput {
    published: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
struct CreateAdminArgs {
    username: String,
    display_name: String,
    password: String,
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "blog_users")]
struct NewUser {
    username: String,
    display_name: String,
    password_hash: String,
    is_admin: bool,
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "blog_posts")]
struct NewPost {
    title: String,
    slug: String,
    excerpt: String,
    body: String,
    image_url: Option<String>,
    author_id: i64,
    published: bool,
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "blog_posts")]
struct PostPatch {
    title: String,
    slug: String,
    excerpt: String,
    body: String,
    image_url: Option<String>,
    published: bool,
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "blog_posts")]
struct PostStatusPatch {
    published: bool,
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "blog_comments")]
struct NewComment {
    post_id: i64,
    author_id: i64,
    body: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
struct UserOut {
    id: i64,
    username: String,
    display_name: String,
    is_admin: bool,
    created_at: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
struct PostOut {
    id: i64,
    title: String,
    slug: String,
    excerpt: String,
    body: String,
    image_url: Option<String>,
    published: bool,
    created_at: String,
    updated_at: String,
    author: UserOut,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
struct CommentOut {
    id: i64,
    post_id: i64,
    body: String,
    created_at: String,
    author: UserOut,
    can_delete: bool,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
struct PostDetailOut {
    post: PostOut,
    comments: Vec<CommentOut>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
struct SessionOut {
    user: Option<UserOut>,
}

struct CookieJson<T>(Response, std::marker::PhantomData<T>);

impl<T> CookieJson<T>
where
    T: Serialize,
{
    fn new(body: T) -> Self {
        Self(Json(body).into_response(), std::marker::PhantomData)
    }

    fn response_mut(&mut self) -> &mut Response {
        &mut self.0
    }
}

impl<T> IntoResponse for CookieJson<T> {
    fn into_response(self) -> Response {
        self.0
    }
}

impl<T> vyuh::callables::IntoReturnPart for CookieJson<T>
where
    T: JsonSchema + Send + 'static,
{
    fn into_return_part() -> vyuh::callables::ReturnPart {
        <Json<T> as vyuh::callables::IntoReturnPart>::into_return_part()
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
struct PageOut<T> {
    items: Vec<T>,
    total: i64,
    page: usize,
    per_page: usize,
    total_pages: usize,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
struct AdminSummaryOut {
    post_count: i64,
    user_count: i64,
    comment_count: i64,
    posts: PageOut<PostOut>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
struct OkOut {
    ok: bool,
}

#[derive(Debug, Clone, Copy)]
struct Pagination {
    page: usize,
    per_page: usize,
}

struct MaybeUser(Option<AuthUser>);

impl FromRequestParts<Site> for MaybeUser {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, site: &Site) -> Result<Self, Self::Rejection> {
        let user = parts
            .extensions
            .get::<AuthUser>()
            .cloned()
            .or_else(|| site.auth().extract_user(parts, &[], false).ok());
        if let Some(user) = user.as_ref() {
            parts.extensions.insert(user.clone());
        }
        Ok(Self(user))
    }
}

impl vyuh::callables::IntoArgPart for MaybeUser {
    fn into_arg_part() -> vyuh::callables::ArgPart {
        vyuh::callables::ArgPart::Optional(Box::new(
            <AuthUser as vyuh::callables::IntoArgPart>::into_arg_part(),
        ))
    }
}

struct MaybeAdmin(bool);

impl FromRequestParts<Site> for MaybeAdmin {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, site: &Site) -> Result<Self, Self::Rejection> {
        let user = MaybeUser::from_request_parts(parts, site).await?.0;
        let is_admin = user
            .map(|user| user.roles & UserRole::Admin.to_role_type() != 0)
            .unwrap_or(false);
        Ok(Self(is_admin))
    }
}

impl vyuh::callables::IntoArgPart for MaybeAdmin {
    fn into_arg_part() -> vyuh::callables::ArgPart {
        vyuh::callables::ArgPart::Optional(Box::new(
            <AdminUser as vyuh::callables::IntoArgPart>::into_arg_part(),
        ))
    }
}

#[bundles::route(path = "/")]
async fn app() -> Result<Html<String>, Error> {
    Ok(Html(include_str!("blog/ui.html").to_string()))
}

#[bundles::route(path = "/api/session")]
async fn session(site: Site, MaybeUser(user): MaybeUser) -> Result<Json<SessionOut>, Error> {
    let Some(user) = user else {
        return Ok(Json(SessionOut { user: None }));
    };
    let mut db = site.db();
    let user = load_user(&mut db, user_id(&user)?).await?;
    Ok(Json(SessionOut {
        user: Some(UserOut::from(user)),
    }))
}

#[bundles::route(path = "/api/session", method = "POST")]
async fn login(
    site: Site,
    Valid(Json(input)): Valid<Json<LoginInput>>,
) -> Result<CookieJson<SessionOut>, Error> {
    let mut db = site.db();
    let user = find_user_by_username(&mut db, &input.username).await?;
    if !check_password(&input.password, &user.password_hash)? {
        return Err(Error::bad_request("invalid username or password"));
    }
    let mut response = CookieJson::new(SessionOut {
        user: Some(UserOut::from(user.clone())),
    });
    let subject = user.id.to_string();
    let roles = UserRole::User.to_role_type()
        | if user.is_admin {
            UserRole::Admin.to_role_type()
        } else {
            0
        };
    site.auth().login_user(
        AuthUser::new(&subject, roles),
        &["blog"],
        response.response_mut(),
    )?;
    Ok(response)
}

#[bundles::route(path = "/api/session", method = "DELETE")]
async fn logout(site: Site) -> Result<CookieJson<OkOut>, Error> {
    let mut response = CookieJson::new(OkOut { ok: true });
    site.auth().logout(false, response.response_mut());
    site.auth().logout(true, response.response_mut());
    Ok(response)
}

#[bundles::route(path = "/api/posts")]
async fn list_posts(
    site: Site,
    MaybeAdmin(is_admin): MaybeAdmin,
    Query(query): Query<PostsQuery>,
) -> Result<Json<PageOut<PostOut>>, Error> {
    let mut db = site.db();
    let include_all = query.all && is_admin;
    let posts = query_posts(
        &mut db,
        include_all,
        page_params(query.page, query.per_page),
    )
    .await?;
    Ok(Json(PageOut::from_page(posts, PostOut::from)))
}

#[bundles::route(path = "/api/posts/{slug}")]
async fn show_post(
    site: Site,
    MaybeUser(user): MaybeUser,
    MaybeAdmin(is_admin): MaybeAdmin,
    Path(slug): Path<String>,
) -> Result<Json<PostDetailOut>, Error> {
    let mut db = site.db();
    let post = load_post_by_slug_with_author(&mut db, &slug).await?;
    if !post.post.published && !is_admin {
        return Err(Error::not_found("post not found"));
    }
    let comments = load_comments(&mut db, post.post.id).await?;
    let actor_id = user.as_ref().map(user_id).transpose()?;
    let actor_is_admin = match actor_id {
        Some(id) => load_user(&mut db, id).await?.is_admin,
        None => false,
    };
    Ok(Json(detail_out(post, comments, actor_id, actor_is_admin)))
}

#[bundles::route(path = "/api/posts", method = "POST")]
async fn create_post(
    site: Site,
    user: AdminUser,
    Valid(MultipartForm(input)): Valid<MultipartForm<PostForm>>,
) -> Result<Json<PostOut>, Error> {
    let mut db = site.db();
    let admin = load_user(&mut db, user_id(&user.into_user())?).await?;
    let image_url = save_post_image(&site, input.image.as_ref()).await?;
    let slug = unique_slug(&mut db, &slugify(&input.title), None).await?;
    let post = insert_post(&mut db, &input, slug, image_url, admin.id).await?;
    Ok(Json(PostOut::from((post, admin))))
}

#[bundles::route(path = "/api/admin/posts/{id}", method = "PUT")]
async fn update_post(
    site: Site,
    user: AdminUser,
    Path(id): Path<i64>,
    Valid(MultipartForm(input)): Valid<MultipartForm<PostForm>>,
) -> Result<Json<PostOut>, Error> {
    let mut db = site.db();
    let _admin = user.into_user();
    let current = load_post(&mut db, id).await?;
    let image_url = save_post_image(&site, input.image.as_ref())
        .await?
        .or(current.image_url);
    let slug = unique_slug(&mut db, &slugify(&input.title), Some(id)).await?;
    update_post_row(&mut db, id, &input, slug, image_url).await?;
    let updated = load_post_with_author(&mut db, id).await?;
    Ok(Json(PostOut::from(updated)))
}

#[bundles::route(path = "/api/admin/posts/{id}/status", method = "PATCH")]
async fn update_status(
    site: Site,
    user: AdminUser,
    Path(id): Path<i64>,
    Json(input): Json<StatusInput>,
) -> Result<Json<PostOut>, Error> {
    let mut db = site.db();
    let _admin = user.into_user();
    let table = Post::table();
    db::from(&table)
        .filter(table.id.eq(db::val(id)))
        .update(&PostStatusPatch {
            published: input.published,
        })
        .exec(&mut db)
        .await?;
    Ok(Json(PostOut::from(
        load_post_with_author(&mut db, id).await?,
    )))
}

#[bundles::route(path = "/api/admin/posts/{id}", method = "DELETE")]
async fn delete_post(
    site: Site,
    user: AdminUser,
    Path(id): Path<i64>,
) -> Result<Json<OkOut>, Error> {
    let mut db = site.db();
    let _admin = user.into_user();
    let table = Post::table();
    db::from(&table)
        .filter(table.id.eq(db::val(id)))
        .delete()
        .exec(&mut db)
        .await?;
    Ok(Json(OkOut { ok: true }))
}

#[bundles::route(path = "/api/posts/{slug}/comments", method = "POST")]
async fn create_comment(
    site: Site,
    user: BlogUser,
    Path(slug): Path<String>,
    Valid(Json(input)): Valid<Json<CommentInput>>,
) -> Result<Json<PostDetailOut>, Error> {
    let mut db = site.db();
    let user = user.into_user();
    let post = load_post_by_slug(&mut db, &slug).await?;
    let author = load_user(&mut db, user_id(&user)?).await?;
    db::from(&Comment::table())
        .insert(&NewComment {
            post_id: post.id,
            author_id: author.id,
            body: input.body.clone(),
        })
        .exec(&mut db)
        .await?;
    let post = load_post_with_author(&mut db, post.id).await?;
    let comments = load_comments(&mut db, post.post.id).await?;
    Ok(Json(detail_out(
        post,
        comments,
        Some(author.id),
        author.is_admin,
    )))
}

#[bundles::route(path = "/api/comments/{id}", method = "DELETE")]
async fn delete_comment(
    site: Site,
    user: BlogUser,
    Path(id): Path<i64>,
) -> Result<Json<OkOut>, Error> {
    let mut db = site.db();
    let user = user.into_user();
    let actor = load_user(&mut db, user_id(&user)?).await?;
    let comment = load_comment(&mut db, id).await?;
    if !actor.is_admin && comment.author_id != actor.id {
        return Err(forbidden(
            "Only comment authors or admins can delete comments.",
        ));
    }
    let table = Comment::table();
    db::from(&table)
        .filter(table.id.eq(db::val(id)))
        .delete()
        .exec(&mut db)
        .await?;
    Ok(Json(OkOut { ok: true }))
}

#[bundles::route(path = "/api/admin/summary")]
async fn admin_summary(
    site: Site,
    user: AdminUser,
    Query(query): Query<PageQuery>,
) -> Result<Json<AdminSummaryOut>, Error> {
    let mut db = site.db();
    let _admin = user.into_user();
    let post_count = db::from(Post::table()).count().exec(&mut db).await?;
    let user_count = db::from(User::table()).count().exec(&mut db).await?;
    let comment_count = db::from(Comment::table()).count().exec(&mut db).await?;
    let posts = query_posts(
        &mut db,
        true,
        page_params(
            query.page,
            Some(query.per_page.unwrap_or(ADMIN_POST_PAGE_SIZE)),
        ),
    )
    .await?;
    Ok(Json(AdminSummaryOut {
        post_count,
        user_count,
        comment_count,
        posts: PageOut::from_page(posts, PostOut::from),
    }))
}

#[bundles::route(path = "/api/users")]
async fn list_users(
    site: Site,
    user: AdminUser,
    Query(query): Query<PageQuery>,
) -> Result<Json<PageOut<UserOut>>, Error> {
    let mut db = site.db();
    let _admin = user.into_user();
    let pagination = page_params(query.page, query.per_page);
    let total = db::from(User::table()).count().exec(&mut db).await?;
    let table = User::table();
    let users = db::from(&table)
        .order_by(table.created_at.desc())
        .slice::<User>(page_offset_usize(pagination), pagination.per_page)
        .exec(&mut db)
        .await?;
    let users = page_from(users, total, pagination);
    Ok(Json(PageOut::from_page(users, UserOut::from)))
}

#[bundles::route(path = "/api/users", method = "POST")]
async fn create_user(
    site: Site,
    user: AdminUser,
    Valid(Json(input)): Valid<Json<UserInput>>,
) -> Result<Json<UserOut>, Error> {
    let mut db = site.db();
    let _admin = user.into_user();
    let user = insert_user(
        &mut db,
        &input.username,
        &input.display_name,
        &input.password,
        input.is_admin,
    )
    .await?;
    Ok(Json(UserOut::from(user)))
}

#[bundles::route(path = "/api/users/{id}", method = "DELETE")]
async fn delete_user(
    site: Site,
    user: AdminUser,
    Path(id): Path<i64>,
) -> Result<Json<OkOut>, Error> {
    let mut db = site.db();
    let admin = load_user(&mut db, user_id(&user.into_user())?).await?;
    if admin.id == id {
        return Err(forbidden("Admins cannot delete their own account."));
    }
    let table = User::table();
    db::from(&table)
        .filter(table.id.eq(db::val(id)))
        .delete()
        .exec(&mut db)
        .await?;
    Ok(Json(OkOut { ok: true }))
}

#[bundles::route(path = "/uploads/{name}")]
async fn uploaded_file(site: Site, Path(name): Path<String>) -> Result<FileResponse, Error> {
    let name = StorageName::new(name).map_err(|err| Error::bad_request(err.to_string()))?;
    let file = site
        .file_storage()
        .open(&name)
        .await
        .map_err(|err| Error::not_found(format!("uploaded file not found: {err}")))?;
    let content_type = mime_guess::from_path(name.as_str()).first_or_octet_stream();
    let body = Body::from_stream(ReaderStream::new(file));
    Ok(FileResponse::new(body, content_type.to_string()))
}

async fn create_admin(site: Site, Data(args): Data<CreateAdminArgs>) -> Result<(), Error> {
    let mut db = site.db();
    let user = insert_user(
        &mut db,
        &args.username,
        &args.display_name,
        &args.password,
        true,
    )
    .await?;
    println!("created admin user {} with id {}", user.username, user.id);
    Ok(())
}

#[bundles::schema]
fn schema() -> db::Schema {
    db::Schema::builder(db::Dialect::Postgres)
        .table::<User>()
        .table::<Post>()
        .table::<Comment>()
        .build()
}

#[bundles::migrations]
fn migrations() -> db::MigrationSource {
    db::root_migration(&MIGRATIONS)
}

fn auth_bundle() -> bundles::Bundle {
    bundles::bundle! {
        session,
        login,
        logout,
    }
    .with_tags(["Authentication"])
}

fn posts_bundle() -> bundles::Bundle {
    bundles::bundle! {
        app,
        uploaded_file,
        list_posts,
        show_post,
        create_post,
        update_post,
        update_status,
        delete_post,
    }
    .with_tags(["Posts"])
}

fn comments_bundle() -> bundles::Bundle {
    bundles::bundle! {
        create_comment,
        delete_comment,
    }
    .with_tags(["Comments"])
}

fn admin_bundle() -> bundles::Bundle {
    bundles::bundle! {
        admin_summary,
    }
    .with_tags(["Admin"])
}

fn users_bundle() -> bundles::Bundle {
    bundles::bundle! {
        list_users,
        create_user,
        delete_user,
    }
    .with_tags(["Users"])
}

fn app_bundle() -> bundles::Bundle {
    bundles::bundle! {
        schema,
        migrations,
    }
    .merge(auth_bundle())
    .merge(posts_bundle())
    .merge(comments_bundle())
    .merge(admin_bundle())
    .merge(users_bundle())
    .merge(bundles::bundle([bundles::command(
        create_admin,
        CommandConf::new("users:create-admin").description("Create an administrator account."),
    )]))
    .with_openapi(openapi_conf())
}

fn openapi_conf() -> bundles::OpenApiConf {
    bundles::OpenApiConf::default()
        .title("Vyuh Blog REST API")
        .version("0.1.0")
        .description("Postgres-backed JSON REST blog with a Vue CDN UI, auth, uploads, migrations, console, and OpenAPI.")
        .tags(openapi_tags())
        .spec("/openapi.json")
        .viewer("/docs")
}

fn openapi_tags() -> Vec<vyuh::apidocs::TagInfo> {
    vec![
        tag("Authentication", "JSON login, logout, and current session."),
        tag("Posts", "Public browsing and admin post management."),
        tag("Comments", "Authenticated comment creation and deletion."),
        tag("Admin", "Administrative summary metrics."),
        tag("Users", "Admin user-management endpoints."),
    ]
}

fn tag(name: &str, description: &str) -> vyuh::apidocs::TagInfo {
    vyuh::apidocs::TagInfo {
        name: name.to_string(),
        description: Some(description.to_string()),
    }
}

#[tokio::main]
async fn main() -> Result<(), SiteError> {
    let secret = std::env::var("VYUH_SECRET_KEY")
        .unwrap_or_else(|_| "vyuh-blog-development-secret-change-me-please".to_string());
    let conf = SiteConf::from_env_with_files()?
        .secret_key(secret)
        .auth(AuthConf::cookie_pair("blog_access", "blog_refresh"))
        .console(ConsoleConf::default().enabled(true))
        .errors(blog_error_conf());
    Site::run(conf, app_bundle()).await
}

fn blog_error_conf() -> ErrorConf {
    ErrorConf::default()
        .json(|ctx, view| async move {
            (
                view.status,
                Json(json!({
                    "source": view.source,
                    "code": view.code,
                    "detail": view.message,
                    "path": ctx.path,
                    "errors": view.errors,
                })),
            )
                .into_response()
        })
        .html(|ctx, view| async move {
            let body = error_html(ctx.method.as_str(), &ctx.path, view.status.as_u16());
            (view.status, Html(body)).into_response()
        })
        .http_mode(HttpErrorRenderMode::Auto)
}

fn error_html(method: &str, path: &str, status: u16) -> String {
    format!(
        r#"<!doctype html><html><head><meta charset="utf-8"><title>{status}</title></head><body><main><h1>{status}</h1><p>{method} {path}</p></main></body></html>"#,
        status = status,
        method = escape_html(method),
        path = escape_html(path),
    )
}

async fn insert_user(
    db: &mut db::DbPool,
    username: &str,
    display_name: &str,
    password: &str,
    is_admin: bool,
) -> Result<User, Error> {
    if find_user_by_username(db, username).await.is_ok() {
        return Err(Error::bad_request("username already exists"));
    }
    let password_hash = make_password(password, None, None)?;
    Ok(db::from(User::table())
        .returning::<User>()
        .insert(&NewUser {
            username: username.to_string(),
            display_name: display_name.to_string(),
            password_hash,
            is_admin,
        })
        .exec(db)
        .await?)
}

async fn insert_post(
    db: &mut db::DbPool,
    input: &PostForm,
    slug: String,
    image_url: Option<String>,
    author_id: i64,
) -> Result<Post, Error> {
    Ok(db::from(Post::table())
        .returning::<Post>()
        .insert(&NewPost {
            title: input.title.clone(),
            slug: slug.clone(),
            excerpt: input.excerpt.clone(),
            body: input.body.clone(),
            image_url,
            author_id,
            published: input.published,
        })
        .exec(db)
        .await?)
}

async fn update_post_row(
    db: &mut db::DbPool,
    id: i64,
    input: &PostForm,
    slug: String,
    image_url: Option<String>,
) -> Result<(), Error> {
    let table = Post::table();
    db::from(&table)
        .filter(table.id.eq(db::val(id)))
        .update(&PostPatch {
            title: input.title.clone(),
            slug,
            excerpt: input.excerpt.clone(),
            body: input.body.clone(),
            image_url,
            published: input.published,
        })
        .exec(db)
        .await?;
    Ok(())
}

async fn query_posts(
    db: &mut db::DbPool,
    include_all: bool,
    pagination: Pagination,
) -> Result<db::Page<PostWithAuthor>, Error> {
    let table = Post::table();
    let total = if include_all {
        db::from(&table).count().exec(db).await?
    } else {
        db::from(&table)
            .filter(table.published.eq(db::val(true)))
            .count()
            .exec(db)
            .await?
    };
    let scope = db::from(&table).order_by(table.created_at.desc());
    let scope = if include_all {
        scope
    } else {
        scope.filter(table.published.eq(db::val(true)))
    };
    let items = scope
        .slice::<PostWithAuthor>(page_offset_usize(pagination), pagination.per_page)
        .exec(db)
        .await?;
    Ok(page_from(items, total, pagination))
}

async fn load_post_with_author(db: &mut db::DbPool, id: i64) -> Result<PostWithAuthor, Error> {
    let table = Post::table();
    Ok(db::from(&table)
        .filter(table.id.eq(db::val(id)))
        .one()
        .exec(db)
        .await?)
}

async fn load_post_by_slug_with_author(
    db: &mut db::DbPool,
    slug: &str,
) -> Result<PostWithAuthor, Error> {
    let table = Post::table();
    Ok(db::from(&table)
        .filter(table.slug.eq(db::val(slug.to_string())))
        .one()
        .exec(db)
        .await?)
}

async fn load_user(db: &mut db::DbPool, id: i64) -> Result<User, Error> {
    let table = User::table();
    Ok(db::from(&table)
        .filter(table.id.eq(db::val(id)))
        .one()
        .exec(db)
        .await?)
}

async fn find_user_by_username(db: &mut db::DbPool, username: &str) -> Result<User, Error> {
    let table = User::table();
    Ok(db::from(&table)
        .filter(table.username.eq(db::val(username.to_string())))
        .one()
        .exec(db)
        .await?)
}

async fn load_post(db: &mut db::DbPool, id: i64) -> Result<Post, Error> {
    let table = Post::table();
    Ok(db::from(&table)
        .filter(table.id.eq(db::val(id)))
        .one()
        .exec(db)
        .await?)
}

async fn load_post_by_slug(db: &mut db::DbPool, slug: &str) -> Result<Post, Error> {
    let table = Post::table();
    Ok(db::from(&table)
        .filter(table.slug.eq(db::val(slug.to_string())))
        .one()
        .exec(db)
        .await?)
}

async fn load_comment(db: &mut db::DbPool, id: i64) -> Result<Comment, Error> {
    let table = Comment::table();
    Ok(db::from(&table)
        .filter(table.id.eq(db::val(id)))
        .one()
        .exec(db)
        .await?)
}

async fn load_comments(db: &mut db::DbPool, post_id: i64) -> Result<Vec<CommentWithAuthor>, Error> {
    let table = Comment::table();
    Ok(db::from(&table)
        .filter(table.post_id.eq(db::val(post_id)))
        .order_by(table.created_at.asc())
        .all()
        .exec(db)
        .await?)
}

async fn unique_slug(
    db: &mut db::DbPool,
    base: &str,
    existing_id: Option<i64>,
) -> Result<String, Error> {
    let base = if base.is_empty() { "post" } else { base };
    for i in 0..100 {
        let candidate = numbered_slug(base, i);
        if !slug_exists(db, &candidate, existing_id).await? {
            return Ok(candidate);
        }
    }
    Err(Error::bad_request("could not generate unique slug"))
}

fn numbered_slug(base: &str, index: usize) -> String {
    if index == 0 {
        base.to_string()
    } else {
        format!("{base}-{index}")
    }
}

async fn slug_exists(
    db: &mut db::DbPool,
    slug: &str,
    existing_id: Option<i64>,
) -> Result<bool, Error> {
    let table = Post::table();
    let scope = db::from(&table).filter(table.slug.eq(db::val(slug.to_string())));
    let exists = match existing_id {
        Some(id) => {
            scope
                .filter(table.id.ne(db::val(id)))
                .exists()
                .exec(db)
                .await?
        }
        None => scope.exists().exec(db).await?,
    };
    Ok(exists)
}

async fn save_post_image(
    site: &Site,
    image: Option<&UploadedFile>,
) -> Result<Option<String>, Error> {
    let Some(image) = image else {
        return Ok(None);
    };
    let saved = site.file_storage().save(image).await?;
    Ok(saved.url)
}

fn detail_out(
    post: PostWithAuthor,
    comments: Vec<CommentWithAuthor>,
    actor_id: Option<i64>,
    actor_is_admin: bool,
) -> PostDetailOut {
    let comments = comments
        .into_iter()
        .map(|comment| CommentOut::from_item(comment, actor_id, actor_is_admin))
        .collect();
    PostDetailOut {
        post: PostOut::from(post),
        comments,
    }
}

impl From<User> for UserOut {
    fn from(user: User) -> Self {
        Self {
            id: user.id,
            username: user.username,
            display_name: user.display_name,
            is_admin: user.is_admin,
            created_at: user.created_at.to_rfc3339(),
        }
    }
}

impl From<PostWithAuthor> for PostOut {
    fn from(item: PostWithAuthor) -> Self {
        Self::from((item.post, item.author))
    }
}

impl From<(Post, User)> for PostOut {
    fn from((post, author): (Post, User)) -> Self {
        Self {
            id: post.id,
            title: post.title,
            slug: post.slug,
            excerpt: post.excerpt,
            body: post.body,
            image_url: post.image_url,
            published: post.published,
            created_at: post.created_at.to_rfc3339(),
            updated_at: post.updated_at.to_rfc3339(),
            author: UserOut::from(author),
        }
    }
}

impl CommentOut {
    fn from_item(item: CommentWithAuthor, actor_id: Option<i64>, is_admin: bool) -> Self {
        let can_delete = is_admin || actor_id == Some(item.comment.author_id);
        Self {
            id: item.comment.id,
            post_id: item.comment.post_id,
            body: item.comment.body,
            created_at: item.comment.created_at.to_rfc3339(),
            author: UserOut::from(item.author),
            can_delete,
        }
    }
}

impl<T> PageOut<T> {
    fn from_page<I>(page: db::Page<I>, map: impl Fn(I) -> T) -> Self {
        Self {
            items: page.items.into_iter().map(map).collect(),
            total: page.total,
            page: page.page,
            per_page: page.per_page,
            total_pages: page.total_pages,
        }
    }
}

fn page_from<T>(items: Vec<T>, total: i64, pagination: Pagination) -> db::Page<T> {
    let total_pages = if pagination.per_page == 0 {
        0
    } else {
        ((total.max(0) as usize) + pagination.per_page - 1) / pagination.per_page
    };
    db::Page {
        items,
        total,
        page: pagination.page,
        per_page: pagination.per_page,
        total_pages,
    }
}

fn page_offset_usize(pagination: Pagination) -> usize {
    (pagination.page - 1) * pagination.per_page
}

fn page_params(page: Option<usize>, per_page: Option<usize>) -> Pagination {
    Pagination {
        page: page.unwrap_or(1).max(1),
        per_page: per_page
            .unwrap_or(DEFAULT_PAGE_SIZE)
            .clamp(1, MAX_PAGE_SIZE),
    }
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn user_id(user: &AuthUser) -> Result<i64, Error> {
    user.key
        .parse::<i64>()
        .map_err(|_| Error::bad_request("invalid user id in token"))
}

fn forbidden(message: &str) -> Error {
    Error::new(ErrorKind::Forbidden).with_context(message.to_string())
}

fn slugify(value: &str) -> String {
    let mut out = String::new();
    let mut dash = false;
    for ch in value.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
            dash = false;
        } else if !dash && !out.is_empty() {
            out.push('-');
            dash = true;
        }
    }
    out.trim_matches('-').to_string()
}
