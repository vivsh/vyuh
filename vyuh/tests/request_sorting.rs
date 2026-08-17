#![cfg(feature = "sqlite")]

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use vyuh::db::Model;
use vyuh::{
    SiteConf, bundles, db,
    routes::{Json, Query, StatusCode},
    testing::TestSite,
};

#[derive(Debug, Clone, db::Model)]
#[table(name = "notes")]
struct Note {
    #[column(primary_key)]
    id: i64,
    title: String,
    created_at: i64,
}

#[derive(Debug, Clone, db::SortKey)]
#[sort(model = Note, max_terms = 2)]
enum NoteSort {
    Title,
    #[sort(name = "newest", by = created_at)]
    Newest,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ListNotes {
    sort: Option<db::Sort<NoteSort>>,
    page: Option<u32>,
    q: Option<String>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
struct SortOut {
    sql: String,
}

#[bundles::route(path = "/notes")]
async fn list_notes(Query(params): Query<ListNotes>) -> Json<SortOut> {
    let ListNotes {
        sort,
        page: _,
        q: _,
    } = params;
    let notes = Note::table();
    let query = db::from(&notes);
    let query = match sort.as_ref() {
        Some(sort) => query.sort_with(sort),
        None => query,
    };
    let sql = match query.all::<Note>().plan() {
        Ok(plan) => plan.sql,
        Err(error) => error.to_string(),
    };
    Json(SortOut { sql })
}

/// Verifies Vyuh query DTOs compose sorting with unrelated request fields.
#[tokio::test]
async fn request_sorting_uses_vyuh_db_facade() {
    let bundle = bundles::bundle! { list_notes }.with_conf(
        bundles::conf().openapi(
            bundles::OpenApiConf::default()
                .title("Sorting")
                .version("0.1.0")
                .spec("/openapi.json")
                .public(),
        ),
    );
    let site = vyuh::Site::build(
        SiteConf {
            log_init: false,
            ..SiteConf::default()
        },
        bundle,
    )
    .await
    .expect("site");
    let client = TestSite::new(site.clone());

    let sorted: Value = client
        .get("/notes?sort=-newest,title&page=2&q=note")
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;
    assert_eq!(
        sorted["sql"],
        "SELECT notes.id, notes.title, notes.created_at FROM notes ORDER BY notes.created_at DESC, notes.title ASC"
    );
    client
        .get("/notes?sort=unknown")
        .send()
        .await
        .assert_status(StatusCode::BAD_REQUEST);
    client
        .get("/notes?sort=unknown&page=2")
        .send()
        .await
        .assert_status(StatusCode::BAD_REQUEST);
    let unordered: Value = client
        .get("/notes?page=2&q=note")
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;
    assert!(
        !unordered["sql"]
            .as_str()
            .is_some_and(|sql| sql.contains(" ORDER BY "))
    );

    let spec: Value = client
        .get("/openapi.json")
        .send()
        .await
        .assert_status(StatusCode::OK)
        .json()
        .await;
    assert!(spec["paths"]["/notes"]["get"].to_string().contains("sort"));
    assert!(
        spec["paths"]["/notes"]["get"]
            .to_string()
            .contains("newest")
    );
    site.shutdown_and_wait().await;
}
