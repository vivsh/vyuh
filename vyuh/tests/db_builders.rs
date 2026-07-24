#![cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]

use vyuh::db::mock::{DbCallKind, MockDBSession, PlannedCall, PlannedResponse};
use vyuh::prelude::*;

#[derive(Debug, Clone, PartialEq, db::Model)]
#[table(name = "users")]
struct User {
    id: i64,
    email: String,
    active: bool,
}

#[derive(Debug, Clone, PartialEq, db::Model)]
#[table(name = "posts")]
struct Post {
    id: i64,
    user_id: i64,
    title: String,
}

#[derive(Debug, Clone, PartialEq, db::Record)]
struct PostWithAuthor {
    #[column(flatten)]
    post: Post,
    #[column(reference(on(from = "user_id", to = "id")))]
    author: User,
}

#[derive(Debug, Clone, db::Record)]
struct UserPatch {
    active: bool,
}

#[derive(Debug, Clone, db::Model)]
#[table(name = "accounts", schema = "auth")]
struct Account {
    #[column(primary_key)]
    id: i64,
    #[column(name = "email_address", type = "citext")]
    email: String,
    #[column(nullable)]
    nickname: Option<String>,
}

#[derive(Debug, Clone, db::Model)]
#[table(
    name = "memberships",
    primary_key(name = "memberships_identity", columns = ["tenant_id", "user_id"])
)]
struct Membership {
    tenant_id: i64,
    user_id: i64,
    role: String,
}

#[derive(Debug, Clone, db::Model)]
struct AuditLog {
    id: i64,
    message: String,
}

#[test]
fn model_derive_exposes_gaman_compatible_metadata() {
    assert_eq!(<Account as db::Record>::record_table_name(), "accounts");
    assert_eq!(<Account as db::Record>::record_table_schema(), Some("auth"));
    assert_eq!(<Account as db::Model>::primary_key_column(), Some("id"));
    assert_eq!(
        <Account as db::Record>::record_column_names(),
        vec![
            "id".to_string(),
            "email_address".to_string(),
            "nickname".to_string()
        ]
    );
}

/// Verifies that `Model` derives Gaman `IntoTable` metadata through the public table builder.
#[test]
fn model_derive_generates_gaman_table() {
    let table = <Account as db::IntoTable>::into_table(&db::Dialect::Postgres);

    assert_eq!(table.name, "accounts");
    assert_eq!(table.schema.as_deref(), Some("auth"));
    assert!(
        table
            .columns
            .iter()
            .any(|c| c.name == "id" && c.primary_key)
    );
    assert!(
        table
            .columns
            .iter()
            .any(|c| c.name == "nickname" && c.nullable)
    );
}

/// Verifies that composite primary keys keep their explicit name and column order.
#[test]
fn model_derive_preserves_composite_primary_key() {
    let table = <Membership as db::IntoTable>::into_table(&db::Dialect::Postgres);
    assert!(table.primary_key.is_some());
    if let Some(primary_key) = table.primary_key.as_ref() {
        assert_eq!(primary_key.name, "memberships_identity");
        assert_eq!(primary_key.columns, vec!["tenant_id", "user_id"]);
    }
    assert_eq!(
        <Membership as db::Model>::primary_key_columns(),
        &["tenant_id", "user_id"]
    );
}

#[test]
fn table_name_defaults_to_snake_case() {
    assert_eq!(<AuditLog as db::Record>::record_table_name(), "audit_log");
    assert_eq!(<AuditLog as db::Model>::table_name(), "audit_log");
}

#[tokio::test]
async fn typed_select_uses_model_metadata() {
    let mut session = MockDBSession::new();
    session.plan(PlannedCall {
        kind: DbCallKind::FetchAll,
        sql_contains: Some("SELECT user.id, user.email, user.active FROM users user"),
        response: PlannedResponse::OkAnyVec(Box::new(Vec::<User>::new())),
    });

    let table = User::table();
    let users = db::from(&table)
        .all::<User>()
        .exec(&mut session)
        .await
        .unwrap();

    assert!(users.is_empty());
    assert_eq!(session.recorded.len(), 1);
}

#[tokio::test]
async fn model_builder_functions_work() {
    let mut session = MockDBSession::new();
    session.plan(PlannedCall {
        kind: DbCallKind::FetchOne,
        sql_contains: Some("FROM users"),
        response: PlannedResponse::OkAny(Box::new((0_i64,))),
    });
    session.plan_execute_ok("INSERT INTO users", 1);
    session.plan_execute_ok("UPDATE users SET", 1);
    session.plan_execute_ok("DELETE FROM users WHERE (id = ", 1);
    session.plan_execute_ok("DELETE FROM users WHERE (id = ", 1);

    let total = db::query("SELECT COUNT(*) FROM users")
        .scalar::<i64>(&mut session)
        .await
        .unwrap();
    assert_eq!(total, 0);

    let table = User::table();
    let id = db::var::<i64>().named("id");

    db::from(&table)
        .insert(&User {
            id: 1,
            email: "a@example.com".to_string(),
            active: true,
        })
        .exec(&mut session)
        .await
        .unwrap();

    db::from(&table)
        .filter(table.id.eq(&id))
        .bind(&id, 1_i64)
        .update(&UserPatch { active: true })
        .exec(&mut session)
        .await
        .unwrap();

    db::from(&table)
        .filter(table.id.eq(&id))
        .bind(&id, 1_i64)
        .delete()
        .exec(&mut session)
        .await
        .unwrap();

    db::from(&table)
        .filter(table.id.eq(&id))
        .bind(&id, 1_i64)
        .delete()
        .exec(&mut session)
        .await
        .unwrap();
}

#[tokio::test]
async fn typed_insert_update_delete_and_raw_query_use_named_binds() {
    let mut session = MockDBSession::new();
    session.plan_execute_ok("INSERT INTO users (id, email, active) VALUES", 1);
    session.plan_execute_ok("UPDATE users SET active = ", 1);
    session.plan_execute_ok("DELETE FROM users WHERE (id = ", 1);
    session.plan_execute_ok("SELECT * FROM users WHERE id = ", 1);

    let table = User::table();
    let id = db::var::<i64>().named("id");

    db::from(&table)
        .insert(&User {
            id: 1,
            email: "a@example.com".to_string(),
            active: true,
        })
        .exec(&mut session)
        .await
        .unwrap();

    db::from(&table)
        .filter(table.id.eq(&id))
        .bind(&id, 1_i64)
        .update(&UserPatch { active: false })
        .exec(&mut session)
        .await
        .unwrap();

    db::from(&table)
        .filter(table.id.eq(&id))
        .bind(&id, 1_i64)
        .delete()
        .exec(&mut session)
        .await
        .unwrap();

    db::query("SELECT * FROM users WHERE id = :id")
        .bind("id", 1_i64)
        .execute(&mut session)
        .await
        .unwrap();
}

#[tokio::test]
async fn typed_select_resolves_implicit_references() {
    let mut session = MockDBSession::new();
    session.plan(PlannedCall {
        kind: DbCallKind::FetchAll,
        sql_contains: Some("JOIN users author ON author.id = post.user_id WHERE (post.id >= "),
        response: PlannedResponse::OkAnyVec(Box::new(Vec::<PostWithAuthor>::new())),
    });

    let table = Post::table();

    let rows = db::from(&table)
        .filter(table.id.gte(db::val(10_i64)))
        .all::<PostWithAuthor>()
        .exec(&mut session)
        .await
        .unwrap();

    assert!(rows.is_empty());
    let sql = &session.recorded[0].stmt.sql;
    assert!(
        sql.contains("SELECT post.id, post.user_id, post.title, author.id, author.email, author.active FROM posts post")
    );
    assert!(sql.contains("JOIN users author ON author.id = post.user_id"));
}
