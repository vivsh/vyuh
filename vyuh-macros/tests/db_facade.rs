use schemars::JsonSchema;
use vyuh::db;
use vyuh::prelude::embed_migrations;

static ROOT_MIGRATIONS: db::EmbeddedMigrations = embed_migrations!("tests/fixtures/migrations");
static CRATE_MIGRATIONS: db::EmbeddedMigrations =
    db::embed_migrations!("tests/fixtures/migrations");

#[derive(Debug, Clone, db::Model)]
#[table(name = "notes")]
struct Note {
    #[column(primary_key)]
    id: i64,
    title: String,
}

#[derive(Debug, Clone, db::Record)]
struct NoteRow {
    id: i64,
    title: String,
}

#[derive(Debug, Clone, db::Filterable)]
#[filter(model = Note)]
struct NoteFilter {
    title: Option<String>,
}

#[derive(JsonSchema)]
struct CreateNote {
    title: String,
}

/// Verifies Vyuh's database derives and migration macro require only the Vyuh facade.
#[test]
fn facade_derives_compile_without_mool() {
    assert_model::<Note>();
    assert_record::<NoteRow>();
    assert_filterable::<NoteFilter>();
    assert_schema::<CreateNote>();
    let root = db::root_migration(&ROOT_MIGRATIONS);
    let child = db::crate_migration("notes", &CRATE_MIGRATIONS);

    let filter = NoteFilter { title: None };
    let input = CreateNote {
        title: "note".to_string(),
    };
    assert!(filter.title.is_none());
    assert_eq!(input.title, "note");
    assert_eq!(root.namespace(), None);
    assert_eq!(child.namespace(), Some("notes"));
}

fn assert_model<T: db::Model>() {}

fn assert_record<T: db::Record>() {}

fn assert_filterable<T: db::Filterable>() {}

fn assert_schema<T: JsonSchema>() {}
