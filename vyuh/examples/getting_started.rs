//! Source snippets for the mdBook Getting Started chapter.
//!
//! The chapter includes anchored regions from this file so the tutorial code is
//! checked by Cargo instead of drifting as inline markdown.

use schemars::JsonSchema;
use vyuh::auth::{
    Audience, AuthConf, AuthError, AuthUser, LoginMethod, PasswordCredentials, PasswordLogin,
    PasswordVerifier, PresentedSecret,
};
use vyuh::commands::CommandConf;
use vyuh::prelude::*;

// ANCHOR: data_types
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
struct Signup {
    #[validate(email)]
    email: String,

    #[validate(min_length = 3, max_length = 80)]
    name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct UserCreated {
    id: i64,
    email: String,
    name: String,
}

const WEB: Audience = Audience::new("web");
const PASSWORD: LoginMethod<PasswordCredentials> = LoginMethod::new("password");

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct PasswordInput {
    email: String,
    password: String,
}

#[derive(Clone, Copy)]
struct DemoPasswords;

impl PasswordVerifier for DemoPasswords {
    async fn verify(
        &self,
        username: &str,
        password: &PresentedSecret,
    ) -> Result<AuthUser, AuthError> {
        if username != "demo@example.com" || password.expose() != "change-me" {
            return Err(AuthError::InvalidCredential);
        }
        Ok(AuthUser::new("user-123"))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct BuildReportJob {
    account_id: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct ReportBuilt {
    location: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct Tick {
    source: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct ReindexArgs {
    scope: String,
}
// ANCHOR_END: data_types

// ANCHOR: routes
#[bundles::route(path = "/users", method = "POST")]
async fn signup(Valid(Data(input)): Valid<Data<Signup>>) -> Result<Data<UserCreated>, Error> {
    Ok(Data::new(UserCreated {
        id: 1,
        email: input.email.clone(),
        name: input.name.clone(),
    }))
}

#[bundles::route(path = "/login", method = "POST")]
async fn login(
    site: Site,
    Json(input): Json<PasswordInput>,
) -> Result<vyuh::auth::LoginResponse, Error> {
    let credentials = PasswordCredentials::new(input.email, input.password);
    Ok(site.auth().via(PASSWORD).login(credentials, &[WEB]).await?)
}

#[bundles::route(path = "/refresh", method = "POST")]
async fn refresh(
    site: Site,
    request: vyuh::routes::Request,
) -> Result<vyuh::auth::LoginResponse, Error> {
    let (parts, _) = request.into_parts();
    Ok(site.auth().refresh(&parts, &[WEB]).await?)
}

#[bundles::route(path = "/me", method = "GET")]
async fn me(user: AuthUser) -> Result<Data<String>, Error> {
    Ok(Data::new(user.key.to_string()))
}
// ANCHOR_END: routes

// ANCHOR: runtime_paths
#[bundles::task]
async fn build_report(Data(job): Data<BuildReportJob>) -> Data<ReportBuilt> {
    Data::new(ReportBuilt {
        location: format!("reports/{}.json", job.account_id),
    })
}

#[bundles::cron(expr = "0 */5 * * * * *")]
async fn heartbeat() -> Data<Tick> {
    Data::new(Tick {
        source: "docs-example".into(),
    })
}

#[bundles::signal]
async fn record_tick(Data(tick): Data<Tick>) -> Result<(), Error> {
    println!("tick from {}", tick.source);
    Ok(())
}

async fn rebuild_index(Data(args): Data<ReindexArgs>) -> Result<(), Error> {
    println!("reindex {}", args.scope);
    Ok(())
}
// ANCHOR_END: runtime_paths

// ANCHOR: api_bundle
fn api_bundle() -> bundles::Bundle {
    bundles::bundle! {
        signup,
        login,
        refresh,
        me,
        build_report,
        heartbeat,
        record_tick,
    }
    .merge(command_bundle())
    .with_openapi(
        bundles::OpenApiConf::default()
            .title("Vyuh Getting Started")
            .version("0.1.0")
            .description("Routes, auth, tasks, commands, and cron.")
            .spec("/openapi.json")
            .viewer("/docs"),
    )
    .with_prefix("/api")
    .with_audience(WEB)
}
// ANCHOR_END: api_bundle

// ANCHOR: command_bundle
fn command_bundle() -> bundles::Bundle {
    bundles::bundle([bundles::command(
        rebuild_index,
        CommandConf::new("search:rebuild").description("Rebuild the search index."),
    )])
}
// ANCHOR_END: command_bundle

// ANCHOR: main
#[tokio::main]
async fn main() -> Result<(), SiteError> {
    let auth = AuthConf::default().method(PASSWORD, PasswordLogin::new(DemoPasswords));

    Site::run(SiteConf::from_env_with_files()?.auth(auth), api_bundle()).await
}
// ANCHOR_END: main
