use vyuh::{db, db::DbConf, prelude::*};

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct Echo {
    message: String,
    count: i64,
}

#[derive(Debug, Serialize, JsonSchema)]
struct Health {
    ok: bool,
}

#[derive(Debug, Serialize, JsonSchema, vyuh::db::Model)]
#[table(name = "items")]
struct Item {
    id: i64,
    name: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ItemPath {
    id: i64,
}

#[bundles::route(path = "/health")]
async fn health() -> Json<Health> {
    Json(Health { ok: true })
}

#[bundles::route(path = "/echo", method = "POST")]
async fn echo(Json(input): Json<Echo>) -> Json<Echo> {
    Json(input)
}

#[bundles::route(path = "/items/{id}")]
async fn get_item(site: Site, Path(ItemPath { id }): Path<ItemPath>) -> Result<Json<Item>, Error> {
    let mut session = site.db();
    let table = Item::table();
    let cols = table.cols();
    let item = db::from(&table)
        .filter(cols.id.eq(db::val(id)))
        .first::<Item, _>(&mut session)
        .await?;
    let Some(item) = item else {
        return Err(Error::not_found("item not found"));
    };
    Ok(Json(item))
}

fn port() -> u16 {
    std::env::var("PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8000)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let db_url = std::env::var("T1_SQLITE_URL")
        .unwrap_or_else(|_| "sqlite:file:t1_vyuh?mode=memory&cache=shared".to_string());
    let bundle = bundles::bundle! { health, echo, get_item };
    let conf = SiteConf::default()
        .host("127.0.0.1")
        .port(port())
        .secret_key("vyuh-bench-secret-key-32-bytes-long")
        .database(DbConf::from_url(&db_url)?);
    Site::serve(conf, bundle).await.map_err(Into::into)
}
