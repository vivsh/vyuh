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
//!
//! Run the HTTP integration tests against disposable PostgreSQL databases:
//!
//! ```sh
//! DATABASE_URL=postgres://postgres:postgres@localhost/postgres \
//!   cargo test -p vyuh --features postgres,migrations,test-support --example blog
//! ```
//!
//! The tests use Vyuh's `testing::TestSite`, which provisions Mool's
//! migration-aware test database and exercises routes without binding an HTTP port.

use axum::{body::Body, extract::Request};
use schemars::JsonSchema;
use tokio_util::io::ReaderStream;
use vyuh::{
    ErrorKind,
    auth::{
        AuthConf, AuthUser, CookieConf, Jwt, Permit, Scope, ScopeExpr, ScopeRule, TokenConf,
        TokenProvider, check_password, make_password,
    },
    commands::CommandConf,
    console::ConsoleConf,
    db::backend::ReturningExt,
    errors::ErrorConf,
    file_storage::StorageName,
    prelude::*,
    routes::{
        CookieJson, FileResponse, MultipartForm, OkJson, OkOut, Page, PageParams, UploadedFile,
    },
    utils::text::{numbered_slug, slugify},
};

const BLOG: vyuh::auth::Audience = vyuh::auth::Audience::new("blog");

fn blog_auth() -> AuthConf {
    AuthConf::empty().provider(
        vyuh::auth::DEFAULT_AUTH_PROVIDER,
        TokenProvider::new(Jwt::hs256_site_secret())
            .without_refresh()
            .access(TokenConf::cookie(CookieConf::new("blog_access"))),
    )
}

static MIGRATIONS: db::EmbeddedMigrations = db::embed_migrations!("examples/blog/migrations");
const DEFAULT_PAGE_SIZE: usize = 9;
const MAX_PAGE_SIZE: usize = 50;
const ADMIN_POST_PAGE_SIZE: usize = 8;

const BLOG_USE: Scope = Scope::of("blog:use");
const BLOG_ADMIN: Scope = Scope::of("blog:admin");
const BLOG_USE_RULE: &[Scope] = &[BLOG_USE];
const BLOG_ADMIN_RULE: &[Scope] = &[BLOG_ADMIN];

struct BlogAccess;

impl ScopeRule for BlogAccess {
    const EXPR: ScopeExpr = ScopeExpr::all(BLOG_USE_RULE);
}

struct AdminAccess;

impl ScopeRule for AdminAccess {
    const EXPR: ScopeExpr = ScopeExpr::all(BLOG_ADMIN_RULE);
}

type BlogUser = Permit<BlogAccess>;
type AdminUser = Permit<AdminAccess>;

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
    #[column(reference = "blog_users.id", index)]
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
    #[column(reference = "blog_posts.id", index)]
    post_id: i64,
    #[column(reference = "blog_users.id", index)]
    author_id: i64,
    body: String,
    #[column(type = "timestamp with time zone", default = "now()")]
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, db::Record)]
struct PostWithAuthor {
    #[column(flatten)]
    post: Post,
    #[column(reference(on(from = "author_id", to = "id")))]
    author: User,
}

#[derive(Debug, Clone, Serialize, Deserialize, db::Record)]
struct CommentWithAuthor {
    #[column(flatten)]
    comment: Comment,
    #[column(reference(on(from = "author_id", to = "id")))]
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
    #[serde(flatten)]
    page: PageParams,
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

#[derive(Debug, Clone, Serialize, JsonSchema)]
struct AdminSummaryOut {
    post_count: i64,
    user_count: i64,
    comment_count: i64,
    posts: Page<PostOut>,
}

#[bundles::route(path = "/")]
async fn app() -> Result<Html<String>, Error> {
    Ok(Html(include_str!("blog/ui.html").to_string()))
}

#[bundles::route(path = "/api/session")]
async fn session(site: Site, user: Option<AuthUser>) -> Result<Json<SessionOut>, Error> {
    let Some(user) = user else {
        return Ok(Json(SessionOut { user: None }));
    };
    let mut db = site.db();
    let table = User::table();
    let user = db::from(&table)
        .filter(table.id.eq(db::val(user.parse_key()?)))
        .one::<User>()
        .exec(&mut db)
        .await?;
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
    let table = User::table();
    let user = db::from(&table)
        .filter(table.username.eq(db::val(input.username.clone())))
        .one::<User>()
        .exec(&mut db)
        .await?;
    if !check_password(&input.password, &user.password_hash)? {
        return Err(Error::bad_request("invalid username or password"));
    }
    let mut response = CookieJson::new(SessionOut {
        user: Some(UserOut::from(user.clone())),
    });
    let subject = user.id.to_string();
    let scopes = std::iter::once(BLOG_USE).chain(user.is_admin.then_some(BLOG_ADMIN));
    let login = site
        .auth()
        .login(AuthUser::new(&subject).with_scopes(scopes), &[BLOG])
        .await?;
    login.write(response.response_mut());
    Ok(response)
}

#[bundles::route(path = "/api/session", method = "DELETE")]
async fn logout(site: Site, request: Request) -> Result<CookieJson<OkOut>, Error> {
    let (parts, _) = request.into_parts();
    let mut response = CookieJson::new(OkOut::ok());
    let logout = site.auth().logout(&parts).await?;
    logout.write(response.response_mut());
    Ok(response)
}

#[bundles::route(path = "/api/posts")]
async fn list_posts(
    site: Site,
    admin: Option<AdminUser>,
    Query(query): Query<PostsQuery>,
) -> Result<Json<Page<PostOut>>, Error> {
    let mut db = site.db();
    let include_all = query.all && admin.is_some();
    let page = query.page.resolve(DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE);
    let posts = query_posts(&mut db, include_all, page).await?;
    Ok(Json(posts.map(PostOut::from)))
}

#[bundles::route(path = "/api/posts/{slug}")]
async fn show_post(
    site: Site,
    user: Option<AuthUser>,
    admin: Option<AdminUser>,
    Path(slug): Path<String>,
) -> Result<Json<PostDetailOut>, Error> {
    let mut db = site.db();
    let post_table = Post::table();
    let post = db::from(&post_table)
        .filter(post_table.slug.eq(db::val(slug)))
        .one::<PostWithAuthor>()
        .exec(&mut db)
        .await?;
    if !post.post.published && admin.is_none() {
        return Err(Error::not_found("post not found"));
    }
    let comment_table = Comment::table();
    let comments = db::from(&comment_table)
        .filter(comment_table.post_id.eq(db::val(post.post.id)))
        .sort(comment_table.created_at.asc())
        .all::<CommentWithAuthor>()
        .exec(&mut db)
        .await?;
    let actor_id = user.as_ref().map(AuthUser::parse_key).transpose()?;
    let actor_is_admin = match actor_id {
        Some(id) => {
            let user_table = User::table();
            db::from(&user_table)
                .filter(user_table.id.eq(db::val(id)))
                .one::<User>()
                .exec(&mut db)
                .await?
                .is_admin
        }
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
    let user_table = User::table();
    let admin = db::from(&user_table)
        .filter(user_table.id.eq(db::val(user.into_user().parse_key()?)))
        .one::<User>()
        .exec(&mut db)
        .await?;
    let image_url = save_post_image(&site, input.image.as_ref()).await?;
    let slug = unique_slug(&mut db, &slugify(&input.title), None).await?;
    let post = db::from(Post::table())
        .returning::<Post>()
        .insert(&NewPost {
            title: input.title.clone(),
            slug,
            excerpt: input.excerpt.clone(),
            body: input.body.clone(),
            image_url,
            author_id: admin.id,
            published: input.published,
        })
        .exec(&mut db)
        .await?;
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
    let table = Post::table();
    let current = db::from(&table)
        .filter(table.id.eq(db::val(id)))
        .one::<Post>()
        .exec(&mut db)
        .await?;
    let image_url = save_post_image(&site, input.image.as_ref())
        .await?
        .or(current.image_url);
    let slug = unique_slug(&mut db, &slugify(&input.title), Some(id)).await?;
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
        .exec(&mut db)
        .await?;
    let updated = db::from(&table)
        .filter(table.id.eq(db::val(id)))
        .one::<PostWithAuthor>()
        .exec(&mut db)
        .await?;
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
        db::from(&table)
            .filter(table.id.eq(db::val(id)))
            .one::<PostWithAuthor>()
            .exec(&mut db)
            .await?,
    )))
}

#[bundles::route(path = "/api/admin/posts/{id}", method = "DELETE")]
async fn delete_post(site: Site, user: AdminUser, Path(id): Path<i64>) -> Result<OkJson, Error> {
    let mut db = site.db();
    let _admin = user.into_user();
    let table = Post::table();
    db::from(&table)
        .filter(table.id.eq(db::val(id)))
        .delete()
        .exec(&mut db)
        .await?;
    Ok(OkJson)
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
    let post_table = Post::table();
    let post = db::from(&post_table)
        .filter(post_table.slug.eq(db::val(slug)))
        .one::<Post>()
        .exec(&mut db)
        .await?;
    let user_table = User::table();
    let author = db::from(&user_table)
        .filter(user_table.id.eq(db::val(user.parse_key()?)))
        .one::<User>()
        .exec(&mut db)
        .await?;
    db::from(&Comment::table())
        .insert(&NewComment {
            post_id: post.id,
            author_id: author.id,
            body: input.body.clone(),
        })
        .exec(&mut db)
        .await?;
    let post = db::from(&post_table)
        .filter(post_table.id.eq(db::val(post.id)))
        .one::<PostWithAuthor>()
        .exec(&mut db)
        .await?;
    let comment_table = Comment::table();
    let comments = db::from(&comment_table)
        .filter(comment_table.post_id.eq(db::val(post.post.id)))
        .sort(comment_table.created_at.asc())
        .all::<CommentWithAuthor>()
        .exec(&mut db)
        .await?;
    Ok(Json(detail_out(
        post,
        comments,
        Some(author.id),
        author.is_admin,
    )))
}

#[bundles::route(path = "/api/comments/{id}", method = "DELETE")]
async fn delete_comment(site: Site, user: BlogUser, Path(id): Path<i64>) -> Result<OkJson, Error> {
    let mut db = site.db();
    let user = user.into_user();
    let user_table = User::table();
    let actor = db::from(&user_table)
        .filter(user_table.id.eq(db::val(user.parse_key()?)))
        .one::<User>()
        .exec(&mut db)
        .await?;
    let table = Comment::table();
    let comment = db::from(&table)
        .filter(table.id.eq(db::val(id)))
        .one::<Comment>()
        .exec(&mut db)
        .await?;
    if !actor.is_admin && comment.author_id != actor.id {
        return Err(forbidden(
            "Only comment authors or admins can delete comments.",
        ));
    }
    db::from(&table)
        .filter(table.id.eq(db::val(id)))
        .delete()
        .exec(&mut db)
        .await?;
    Ok(OkJson)
}

#[bundles::route(path = "/api/admin/summary")]
async fn admin_summary(
    site: Site,
    user: AdminUser,
    Query(query): Query<PageParams>,
) -> Result<Json<AdminSummaryOut>, Error> {
    let mut db = site.db();
    let _admin = user.into_user();
    let post_count = db::from(Post::table()).count().exec(&mut db).await?;
    let user_count = db::from(User::table()).count().exec(&mut db).await?;
    let comment_count = db::from(Comment::table()).count().exec(&mut db).await?;
    let page = query.resolve(ADMIN_POST_PAGE_SIZE, MAX_PAGE_SIZE);
    let posts = query_posts(&mut db, true, page).await?;
    Ok(Json(AdminSummaryOut {
        post_count,
        user_count,
        comment_count,
        posts: posts.map(PostOut::from),
    }))
}

#[bundles::route(path = "/api/users")]
async fn list_users(
    site: Site,
    user: AdminUser,
    Query(query): Query<PageParams>,
) -> Result<Json<Page<UserOut>>, Error> {
    let mut db = site.db();
    let _admin = user.into_user();
    let page = query.resolve(DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE);
    let table = User::table();
    let users = db::from(&table)
        .sort(table.created_at.desc())
        .page::<User, _>(
            db::Pagination {
                page_num: page.page,
                page_size: page.per_page,
            },
            &mut db,
        )
        .await?;
    Ok(Json(users.map(UserOut::from)))
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
async fn delete_user(site: Site, user: AdminUser, Path(id): Path<i64>) -> Result<OkJson, Error> {
    let mut db = site.db();
    let table = User::table();
    let admin = db::from(&table)
        .filter(table.id.eq(db::val(user.into_user().parse_key()?)))
        .one::<User>()
        .exec(&mut db)
        .await?;
    if admin.id == id {
        return Err(forbidden("Admins cannot delete their own account."));
    }
    db::from(&table)
        .filter(table.id.eq(db::val(id)))
        .delete()
        .exec(&mut db)
        .await?;
    Ok(OkJson)
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
fn schema() -> Result<db::Schema, db::SchemaLoadError> {
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
    .with_audience(BLOG)
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
        .auth(blog_auth())
        .console(ConsoleConf::default().enabled(true))
        .errors(ErrorConf::default().api_json());
    Site::run(conf, app_bundle()).await
}

async fn insert_user(
    db: &mut db::DbPool,
    username: &str,
    display_name: &str,
    password: &str,
    is_admin: bool,
) -> Result<User, Error> {
    let table = User::table();
    if db::from(&table)
        .filter(table.username.eq(db::val(username.to_string())))
        .exists()
        .exec(db)
        .await?
    {
        return Err(Error::bad_request("username already exists"));
    }
    let password_hash = make_password(password, None, None)?;
    Ok(db::from(&table)
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

async fn query_posts(
    db: &mut db::DbPool,
    include_all: bool,
    page: vyuh::routes::PageBounds,
) -> Result<db::Page<PostWithAuthor>, Error> {
    let table = Post::table();
    let scope = db::from(&table).sort(table.created_at.desc());
    let scope = if include_all {
        scope
    } else {
        scope.filter(table.published.eq(db::val(true)))
    };
    Ok(scope
        .page::<PostWithAuthor, _>(
            db::Pagination {
                page_num: page.page,
                page_size: page.per_page,
            },
            db,
        )
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

fn forbidden(message: &str) -> Error {
    Error::new(ErrorKind::Forbidden).with_context(message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use thiserror::Error as ThisError;
    use vyuh::testing::{TestSite, TestSiteError};

    #[derive(Debug, ThisError)]
    enum TestError {
        #[error(transparent)]
        Blog(#[from] Error),
        #[error(transparent)]
        Db(#[from] db::DbError),
        #[error(transparent)]
        Sql(#[from] db::sqlx::Error),
        #[error(transparent)]
        Site(#[from] TestSiteError),
    }

    /// Returns test-safe application configuration using the configured PostgreSQL test server.
    fn test_conf() -> Result<SiteConf, TestError> {
        Ok(SiteConf::default()
            .secret_key("vyuh-blog-test-secret-key-with-enough-entropy")
            .auth(blog_auth())
            .errors(ErrorConf::default().api_json())
            .log_init(false)
            .database(db::DbConf::from_env()?))
    }

    /// Seeds published and draft posts through the same Mool queries used by the application.
    async fn seed_posts(site: &Site) -> Result<(), Error> {
        let mut db = site.db();
        let author = insert_user(&mut db, "writer", "Writer", "password-123", false).await?;
        let table = Post::table();
        for (title, slug, published) in [
            ("Published post", "published-post", true),
            ("Draft post", "draft-post", false),
        ] {
            db::from(&table)
                .insert(&NewPost {
                    title: title.to_string(),
                    slug: slug.to_string(),
                    excerpt: "Example excerpt".to_string(),
                    body: "Example body".to_string(),
                    image_url: None,
                    author_id: author.id,
                    published,
                })
                .exec(&mut db)
                .await?;
        }
        Ok(())
    }

    /// Verifies that Mool-isolated data is exercised through Vyuh's in-process test site.
    #[vyuh::test(conf = test_conf, bundle = app_bundle)]
    async fn public_posts_hide_drafts(site: &TestSite) -> Result<(), TestError> {
        seed_posts(site.site()).await?;

        let response = site.get("/api/posts").send().await.assert_ok();
        let body: Value = response.json().await;
        assert_eq!(body.pointer("/total").and_then(Value::as_i64), Some(1));
        assert_eq!(
            body.pointer("/items/0/slug").and_then(Value::as_str),
            Some("published-post")
        );

        Ok(())
    }

    /// Verifies that a test site can intentionally leave registered migrations unapplied.
    #[vyuh::test(conf = test_conf, bundle = app_bundle, migrations = false)]
    async fn client_can_skip_migrations(site: &TestSite) -> Result<(), TestError> {
        let table: Option<String> =
            db::sqlx::query_scalar("SELECT to_regclass('public.blog_users')::text")
                .fetch_one(site.site().db().as_sqlx())
                .await?;
        assert_eq!(table, None);

        Ok(())
    }

    /// Verifies that the blog login route returns a session cookie for a valid user.
    #[vyuh::test(conf = test_conf, bundle = app_bundle)]
    async fn login_returns_session_cookie(site: &TestSite) -> Result<(), TestError> {
        let mut db = site.site().db();
        insert_user(&mut db, "reader", "Reader", "password-123", false).await?;

        let response = site
            .post("/api/session")
            .json(&LoginInput {
                username: "reader".to_string(),
                password: "password-123".to_string(),
            })
            .send()
            .await
            .assert_ok();
        let cookie = response
            .header("set-cookie")
            .and_then(|value| value.to_str().ok());
        assert!(cookie.is_some_and(|value| value.starts_with("blog_access=")));

        Ok(())
    }
}
