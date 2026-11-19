//! OAuth persistence. Grant transactions remain independent of client state and replay claims.

use uuid::Uuid;

use crate::db::{self, DbError, DbPool, DbSession, DbTransaction, Statement};

pub(super) const CODE: i16 = 0;
pub(super) const ACCESS: i16 = 1;
pub(super) const REFRESH: i16 = 2;

#[derive(db::Model)]
#[table(name = "vyuh_oauth_grants")]
pub(super) struct GrantRow {
    #[column(primary_key)]
    pub(super) id: Uuid,
    #[column(type = "varchar(512)")]
    pub(super) issuer: String,
    pub(super) client_id: Option<String>,
    pub(super) client_binding: Option<i16>,
    pub(super) subject: Option<String>,
    pub(super) resource: Option<String>,
    pub(super) scopes: Option<String>,
    pub(super) redirect_uri: Option<String>,
    pub(super) pkce_challenge: Option<String>,
    pub(super) code_expires_at: Option<i64>,
    pub(super) expires_at: i64,
    pub(super) generation: Option<i64>,
    pub(super) revoked_at: Option<i64>,
    pub(super) approved: bool,
}

#[derive(db::Model)]
#[table(name = "vyuh_oauth_credentials")]
pub(super) struct CredentialRow {
    #[column(primary_key, type = "varchar(64)")]
    pub(super) digest: String,
    pub(super) grant_id: Uuid,
    pub(super) kind: i16,
    pub(super) generation: i64,
    pub(super) expires_at: i64,
}

#[derive(db::Model)]
#[table(name = "vyuh_oauth_registrations")]
pub(super) struct RegistrationRow {
    #[column(primary_key)]
    pub(super) id: Uuid,
    #[column(type = "varchar(512)")]
    pub(super) issuer: String,
    pub(super) slot: i32,
    pub(super) redirects: String,
    #[column(type = "varchar(32)")]
    pub(super) auth_method: String,
    #[column(type = "varchar(64)")]
    pub(super) secret_digest: Option<String>,
    pub(super) allow_refresh: bool,
    pub(super) scopes: String,
    pub(super) issued_at: i64,
}

#[derive(db::Model)]
#[table(name = "vyuh_oauth_assertions")]
pub(super) struct AssertionRow {
    #[column(primary_key, type = "varchar(64)")]
    pub(super) digest: String,
    #[column(type = "varchar(512)")]
    pub(super) issuer: String,
    pub(super) expires_at: i64,
}

/// Returns the stable migration-managed schema for an issuer, regardless of its enabled modes.
pub(crate) fn schema() -> Result<db::Schema, db::SchemaLoadError> {
    let mut schema = db::schema()
        .model::<GrantRow>()
        .model::<CredentialRow>()
        .model::<RegistrationRow>()
        .model::<AssertionRow>()
        .build()?;
    if let Some(grants) = schema.tables.get_mut("vyuh_oauth_grants") {
        grants.indexes.push(index(
            "idx_vyuh_oauth_expiry",
            &["issuer", "expires_at", "id"],
        ));
    }
    if let Some(credentials) = schema.tables.get_mut("vyuh_oauth_credentials") {
        credentials
            .indexes
            .push(index("idx_vyuh_oauth_grant", &["grant_id"]));
        credentials.foreign_keys.push(db::schema::ForeignKey {
            name: "fk_vyuh_oauth_credential_grant".into(),
            columns: vec!["grant_id".into()],
            to_table: "vyuh_oauth_grants".into(),
            to_columns: vec!["id".into()],
            on_delete: Some("cascade".into()),
            on_update: None,
        });
    }
    if let Some(registrations) = schema.tables.get_mut("vyuh_oauth_registrations") {
        let mut slots = index("idx_vyuh_oauth_registration_slot", &["issuer", "slot"]);
        slots.unique = true;
        registrations.indexes.push(slots);
    }
    if let Some(assertions) = schema.tables.get_mut("vyuh_oauth_assertions") {
        assertions.indexes.push(index(
            "idx_vyuh_oauth_assertion_expiry",
            &["issuer", "expires_at", "digest"],
        ));
    }
    #[cfg(feature = "postgres")]
    let dialect = db::Dialect::Postgres;
    #[cfg(feature = "sqlite")]
    let dialect = db::Dialect::Sqlite;
    #[cfg(feature = "mysql")]
    let dialect = db::Dialect::Mysql;
    schema.prepare_loaded(dialect)
}

fn index(name: &str, columns: &[&str]) -> db::Index {
    db::Index {
        name: name.into(),
        columns: columns.iter().map(|value| (*value).into()).collect(),
        ..Default::default()
    }
}

/// Indexed credential and grant projection. Denials have no credential row.
pub(super) struct Proof {
    pub(super) digest: String,
    pub(super) grant: Uuid,
    pub(super) kind: i16,
    pub(super) credential_generation: i64,
    pub(super) credential_expiry: i64,
    pub(super) generation: Option<i64>,
    pub(super) grant_expiry: i64,
    pub(super) revoked: Option<i64>,
    pub(super) client: String,
    pub(super) client_binding: Option<i16>,
    pub(super) subject: String,
    pub(super) resource: String,
    pub(super) scopes: String,
    pub(super) redirect: String,
    pub(super) challenge: String,
    pub(super) code_expiry: i64,
}

type ProofRow = (
    Uuid,
    i16,
    i64,
    i64,
    Option<i64>,
    i64,
    Option<i64>,
    String,
    Option<i16>,
    String,
    String,
    String,
    String,
    String,
    i64,
);

pub(super) type AccessRow = (String, String, Option<i16>, String, String, String);

/// Fetches only resource-authorization data on the hot path, in one indexed join.
pub(super) async fn access(
    mut pool: DbPool,
    issuer: &str,
    digest: String,
    now: i64,
) -> Result<Option<AccessRow>, DbError> {
    let sql = "SELECT g.subject,g.client_id,g.client_binding,g.resource,g.scopes,g.redirect_uri \
        FROM vyuh_oauth_credentials c JOIN vyuh_oauth_grants g ON c.grant_id=g.id \
        WHERE c.digest=? AND g.issuer=? AND c.kind=? AND g.approved=TRUE \
        AND g.revoked_at IS NULL AND g.generation=c.generation \
        AND c.expires_at>? AND g.expires_at>?";
    pool.fetch_optional(
        statement(sql)
            .bind(digest)
            .bind(issuer.to_owned())
            .bind(ACCESS)
            .bind(now)
            .bind(now),
    )
    .await
}

/// Reads immutable proof bindings and current generation without acquiring locks.
pub(super) async fn lookup(
    mut pool: DbPool,
    issuer: &str,
    digest: String,
) -> Result<Option<Proof>, DbError> {
    let sql = "SELECT g.id,c.kind,c.generation,c.expires_at,g.generation,g.expires_at,\
        g.revoked_at,g.client_id,g.client_binding,g.subject,g.resource,g.scopes,g.redirect_uri,g.pkce_challenge,\
        g.code_expires_at FROM vyuh_oauth_credentials c JOIN vyuh_oauth_grants g \
        ON c.grant_id=g.id WHERE c.digest=? AND g.issuer=? AND g.approved=TRUE";
    let row: Option<ProofRow> = pool
        .fetch_optional(statement(sql).bind(digest.clone()).bind(issuer.to_owned()))
        .await?;
    Ok(row.map(
        |(
            grant,
            kind,
            credential_generation,
            credential_expiry,
            generation,
            grant_expiry,
            revoked,
            client,
            client_binding,
            subject,
            resource,
            scopes,
            redirect,
            challenge,
            code_expiry,
        )| Proof {
            digest,
            grant,
            kind,
            credential_generation,
            credential_expiry,
            generation,
            grant_expiry,
            revoked,
            client,
            client_binding,
            subject,
            resource,
            scopes,
            redirect,
            challenge,
            code_expiry,
        },
    ))
}

/// Consumes the grant ID even on denial; a concurrent decision cannot win twice.
pub(super) async fn decide(
    pool: DbPool,
    grant: &GrantRow,
    code: Option<&CredentialRow>,
) -> Result<bool, DbError> {
    let mut tx = pool.begin().await?;
    let table = <GrantRow as db::Model>::table();
    match db::from(&table).insert(grant).exec(&mut tx).await {
        Ok(_) => {}
        Err(DbError::Integrity {
            kind: db::IntegrityKind::Unique,
            ..
        }) => {
            tx.rollback().await?;
            return Ok(false);
        }
        Err(error) => return Err(error),
    }
    if let Some(code) = code {
        insert_credentials(&mut tx, std::slice::from_ref(code)).await?;
    }
    tx.commit().await?;
    Ok(true)
}

/// Atomically advances one grant and only then inserts replacement credentials.
pub(super) async fn advance(
    pool: DbPool,
    proof: &Proof,
    credentials: &[CredentialRow],
    now: i64,
) -> Result<bool, DbError> {
    let mut tx = pool.begin().await?;
    let predicate = if proof.kind == CODE {
        "generation IS NULL AND code_expires_at>?"
    } else {
        "generation=?"
    };
    let sql = format!(
        "UPDATE vyuh_oauth_grants SET generation=? WHERE id=? \
        AND revoked_at IS NULL AND expires_at>? AND {predicate}"
    );
    let previous = if proof.kind == CODE {
        now
    } else {
        proof.credential_generation
    };
    let next = proof.credential_generation + 1;
    if tx
        .execute(
            statement(&sql)
                .bind(next)
                .bind(proof.grant)
                .bind(now)
                .bind(previous),
        )
        .await?
        == 0
    {
        tx.rollback().await?;
        return Ok(false);
    }
    insert_credentials(&mut tx, credentials).await?;
    tx.commit().await?;
    Ok(true)
}

async fn insert_credentials(
    tx: &mut DbTransaction<'_>,
    credentials: &[CredentialRow],
) -> Result<(), DbError> {
    let table = <CredentialRow as db::Model>::table();
    db::from(&table).insert_many(credentials).exec(tx).await?;
    Ok(())
}

/// Revokes one bound grant. No credential row needs to be updated or locked first.
pub(super) async fn revoke(mut pool: DbPool, grant: Uuid, now: i64) -> Result<(), DbError> {
    pool.execute(
        statement(
            "UPDATE vyuh_oauth_grants SET revoked_at=? \
        WHERE id=? AND revoked_at IS NULL",
        )
        .bind(now)
        .bind(grant),
    )
    .await?;
    Ok(())
}

/// Deletes up to a bounded number of expired grants and their credential history.
pub(super) async fn prune(
    mut pool: DbPool,
    issuer: &str,
    now: i64,
    limit: u32,
) -> Result<u64, DbError> {
    let select = "SELECT id FROM vyuh_oauth_grants WHERE issuer=? AND expires_at<=? \
        ORDER BY expires_at,id LIMIT ?";
    // MySQL requires a materialized derived table when reading the deletion target.
    let sql = format!(
        "DELETE FROM vyuh_oauth_grants WHERE id IN \
        (SELECT id FROM ({select}) AS expired_grants) AND expires_at<=?"
    );
    pool.execute(
        statement(&sql)
            .bind(issuer.to_owned())
            .bind(now)
            .bind(i64::from(limit))
            .bind(now),
    )
    .await
}

pub(super) const REGISTRATION_CAP: i32 = 1024;

/// Finds a candidate slot. Only a subsequent unique insert claims it.
pub(super) async fn free_slot(mut pool: DbPool, issuer: &str) -> Result<Option<i32>, DbError> {
    let sql = "SELECT slot FROM vyuh_oauth_registrations WHERE issuer=? AND slot>=0 \
        AND slot<? ORDER BY slot LIMIT ?";
    let occupied: Vec<(i32,)> = pool
        .fetch_all(
            statement(sql)
                .bind(issuer.to_owned())
                .bind(REGISTRATION_CAP)
                .bind(REGISTRATION_CAP),
        )
        .await?;
    let mut slots = occupied.into_iter().map(|(slot,)| slot).peekable();
    Ok((0..REGISTRATION_CAP).find(|slot| {
        if slots.peek() == Some(slot) {
            slots.next();
            false
        } else {
            true
        }
    }))
}

/// Inserts immutable registration terms. A unique-key race leaves no partial registration.
pub(super) async fn register(mut pool: DbPool, row: &RegistrationRow) -> Result<bool, DbError> {
    let table = <RegistrationRow as db::Model>::table();
    inserted(db::from(&table).insert(row).exec(&mut pool).await)
}

pub(super) type Registration = (String, String, Option<String>, bool, String);

/// Loads only the registration terms used to authorize subsequent requests.
pub(super) async fn registration(
    mut pool: DbPool,
    issuer: &str,
    id: Uuid,
) -> Result<Option<Registration>, DbError> {
    let sql = "SELECT redirects,auth_method,secret_digest,allow_refresh,scopes \
        FROM vyuh_oauth_registrations WHERE id=? AND issuer=?";
    pool.fetch_optional(statement(sql).bind(id).bind(issuer.to_owned()))
        .await
}

/// Disables one client in a standalone delete. Its grant history ages out separately.
pub(super) async fn disable(mut pool: DbPool, issuer: &str, id: Uuid) -> Result<bool, DbError> {
    let sql = "DELETE FROM vyuh_oauth_registrations WHERE id=? AND issuer=?";
    Ok(pool
        .execute(statement(sql).bind(id).bind(issuer.to_owned()))
        .await?
        == 1)
}

/// Claims a signed assertion exactly once across every replica sharing this store.
pub(super) async fn claim_assertion(mut pool: DbPool, row: &AssertionRow) -> Result<bool, DbError> {
    let table = <AssertionRow as db::Model>::table();
    inserted(db::from(&table).insert(row).exec(&mut pool).await)
}

/// Distinguishes a uniqueness loss from all other errors, including ambiguous acknowledgements.
fn inserted(result: Result<u64, DbError>) -> Result<bool, DbError> {
    match result {
        Ok(count) => Ok(count == 1),
        Err(DbError::Integrity {
            kind: db::IntegrityKind::Unique,
            ..
        }) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Deletes an issuer-scoped, bounded batch of assertions past the replay window.
pub(super) async fn prune_assertions(
    mut pool: DbPool,
    issuer: &str,
    now: i64,
    limit: u32,
) -> Result<u64, DbError> {
    let sql = "DELETE FROM vyuh_oauth_assertions WHERE digest IN \
        (SELECT digest FROM (SELECT digest FROM vyuh_oauth_assertions WHERE issuer=? \
        AND expires_at<=? ORDER BY expires_at,digest LIMIT ?) AS expired_assertions) \
        AND expires_at<=?";
    pool.execute(
        statement(sql)
            .bind(issuer.to_owned())
            .bind(now)
            .bind(i64::from(limit))
            .bind(now),
    )
    .await
}

/// Converts static bind markers for PostgreSQL; caller values never enter SQL.
pub(super) fn statement(sql: &str) -> Statement {
    #[cfg(feature = "postgres")]
    {
        let mut parts = sql.split('?');
        let mut text = parts.next().unwrap_or_default().to_owned();
        for (index, part) in parts.enumerate() {
            use std::fmt::Write;
            let _ = write!(text, "${}{}", index + 1, part);
        }
        Statement::raw(&text)
    }
    #[cfg(not(feature = "postgres"))]
    Statement::raw(sql)
}
