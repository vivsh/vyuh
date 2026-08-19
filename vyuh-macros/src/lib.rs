mod assets;
mod beacon;
mod bundle;
mod bundlepart;
mod cron;
mod mcp_tool;
mod migrations;
mod multipart;
mod openapi;
mod periodic;
mod pgnotify;
mod route;
mod schema;
mod schemable;
mod service;
mod signal;
mod task;
mod test;
mod url_info;
mod validate;

use proc_macro::TokenStream;
extern crate proc_macro;

/// Derives the Validate trait for data validation.
///
/// Generates validation logic based on `#[validate(...)]` attributes.
///
/// # Attributes
///
/// ## `#[validate(...)]`
/// - `delegate` - Delegate validation to the field's type (must implement `Validate`)
/// - `custom = "path"` - Call a custom validation function: `fn(&T) -> Result<(), ValidationError>`
/// - `custom_schema = "name"` - Emit `x-vyuh-validators: ["name"]` for custom validation
/// - String: `min_length`, `max_length`, `exact_length`, `pattern`
/// - String formats: `email`, `url`, `uuid`, `phone_e164`, `ipv4`, `ipv6`, `date`, `datetime`
/// - Numeric: `min`, `max`, `exclusive_min`, `exclusive_max`, `multiple_of`
/// - Array: `min_items`, `max_items`, `unique_items`
#[proc_macro_derive(Validate, attributes(validate))]
pub fn derive_validate(input: TokenStream) -> TokenStream {
    validate::derive_validate_impl(input)
}

/// Derives multipart form parsing for named structs.
///
/// Supports `String` text fields and `UploadedFile` file fields. File fields
/// can use `#[upload(...)]` for content type, extension, sniffing, and size
/// rules.
#[proc_macro_derive(MultipartData, attributes(upload))]
pub fn derive_multipart_data(input: TokenStream) -> TokenStream {
    multipart::derive_multipart_data(input)
}

/// Defines a route handler with metadata for routing and OpenAPI documentation.
///
/// This macro is sugar over `vyuh::bundles::route(handler, RouteConf)`.
/// Use the direct API when routes are generated conditionally or when macro
/// syntax is not convenient.
///
/// # Required Attributes
///
/// - `path` - Axum path pattern with optional parameters in braces: `"/users/{id}"`
///
/// # Optional Attributes
///
/// - `method` - HTTP method. Defaults to `"GET"` and can be repeated for
///   multi-method routes.
/// - `name` - Route name for reverse routing (defaults to function name)
/// - `description` - Detailed description for OpenAPI. Defaults to doc comments.
/// - `arg(...)` - Override OpenAPI argument metadata by position/name.
/// - `returns(...)` - Override or append OpenAPI response metadata.
///
/// # Examples
///
/// ```ignore
/// // Free function
/// #[route(path = "/users/{id}")]
/// async fn get_user(Path(id): Path<i32>) -> Json<User> {
///     // ...
/// }
///
/// // Multi-method route
/// #[route(path = "/users", method = "GET", method = "HEAD")]
/// async fn users() -> Json<Vec<User>> {
///     // ...
/// }
///
/// // OpenAPI metadata overrides
/// #[route(
///     path = "/users",
///     method = "POST",
///     returns(status = 201, description = "Created user")
/// )]
/// async fn create_user(Json(input): Json<CreateUser>) -> Json<User> {
///     // ...
/// }
///
/// ```
#[proc_macro_attribute]
pub fn route(attr: TokenStream, item: TokenStream) -> TokenStream {
    route::parse_route(attr, item)
}

/// Defines one declarative authenticated Beacon route.
///
/// This is sugar over `vyuh::bundles::beacon(factory(), BeaconConf::new(...))`.
/// The factory returns a [`vyuh::channels::Beacon`] whose typed rules consume
/// emitted signals. `path` is required; `modes = [ws, sse, poll]`, `name`, and
/// `slash` are optional.
///
/// ```ignore
/// #[bundles::beacon(path = "/live", modes = [ws, sse])]
/// fn live() -> Beacon {
///     Beacon::builder().rule::<NoteChanged>(["notes:read"]).build()
/// }
/// ```
#[proc_macro_attribute]
pub fn beacon(attr: TokenStream, item: TokenStream) -> TokenStream {
    beacon::parse_beacon(attr, item)
}

/// Defines a semantic MCP-only tool backed by a typed Vyuh callable.
///
/// This macro is sugar over `vyuh::bundles::mcp_tool`. The handler must end in
/// one `Data<T>` or `Valid<Data<T>>` object payload. Supported annotations are
/// `read_only`, `destructive`, `idempotent`, and `open_world`.
///
/// ```ignore
/// #[mcp_tool(read_only = true, idempotent = true)]
/// async fn search_notes(
///     _permit: Permit<ReadNotes>,
///     input: Data<SearchNotesInput>,
/// ) -> Result<Data<SearchNotesOutput>, Error> {
///     // ...
/// }
/// ```
#[proc_macro_attribute]
pub fn mcp_tool(attr: TokenStream, item: TokenStream) -> TokenStream {
    mcp_tool::parse_mcp_tool(attr, item)
}

/// Collects bundle parts (routes, tasks, signals) into a Bundle for composition and registration.
///
/// Bundles are the primary unit for organizing and composing application components.
/// Each handler must be annotated with appropriate macros (`#[route]`, `#[cron]`, `#[periodic]`, etc.).
///
/// # Syntax
///
/// ```ignore
/// bundle! {
///     handler1,
///     handler2,
///     ...,
///     tags = ["tag1", "tag2"]  // optional, applies only to routes
/// }
/// ```
///
/// # Options
///
/// - `tags` - Optional array of tags to apply to all routes in the bundle.
///   These tags extend (not replace) any tags defined on individual routes.
///   Note: tags only apply to route parts, not other bundle parts.
///
/// # Examples
///
/// ```ignore
/// // Bundle without tags
/// let user_bundle = bundle! {
///     get_user,        // #[route]
///     create_user,     // #[route]
///     sync_users,      // #[cron]
/// };
///
/// // Bundle with tags - extends individual route tags
/// let api_bundle = bundle! {
///     tags = ["api", "v1"],
///     get_user,
///     create_user,
/// };
///
/// // Compose bundles
/// let all_bundles = bundle! {
///     user_bundle,
///     api_bundle,
/// };
/// ```
///
/// # Notes
///
/// - Handlers must be annotated with `#[route]`, `#[cron]`, `#[periodic]`, `#[pgnotify]`, or `#[signal]`
/// - Handlers can be free functions or references to IntoBundle types
/// - Tags are additive and only apply to route parts
/// - Returns a `Bundle` that implements `IntoBundle`
#[proc_macro]
pub fn bundle(input: TokenStream) -> TokenStream {
    bundle::parse_bundle(input)
}

/// Embeds YAML migrations using Vyuh's database facade.
///
/// The migration directory is resolved relative to the application crate's
/// `CARGO_MANIFEST_DIR`. Generated types resolve through `vyuh::db`, so the
/// application does not need a direct Mool dependency.
#[proc_macro]
pub fn embed_migrations(input: TokenStream) -> TokenStream {
    mool_macros_impl::embed_migrations::expand(input.into(), quote::quote!(::vyuh::db)).into()
}

/// Embeds one asset directory using Vyuh's asset facade.
///
/// Generated asset runtime types resolve through `vyuh::embed`, so
/// applications do not need a direct Rust Silos dependency.
#[proc_macro]
pub fn embed_assets(input: TokenStream) -> TokenStream {
    let silo =
        rust_silos_macros_impl::embed_silo::expand(input.into(), quote::quote!(::vyuh::embed));
    quote::quote!(::vyuh::embed::Dir::new(#silo)).into()
}

#[proc_macro_derive(Record, attributes(field, column, table, db))]
pub fn derive_record(input: TokenStream) -> TokenStream {
    mool_macros_impl::record::derive_record(input.into(), quote::quote!(::vyuh::db)).into()
}

/// Derives physical-column mapping for migration-managed record values.
///
/// Generated implementations resolve through Vyuh's database facade, so an
/// application does not need a direct Mool dependency.
#[proc_macro_derive(ManagedRecord, attributes(column, table, db))]
pub fn derive_managed_record(input: TokenStream) -> TokenStream {
    mool_macros_impl::record::derive_managed_record(input.into(), quote::quote!(::vyuh::db)).into()
}

#[proc_macro_derive(Model, attributes(field, column, table, db))]
pub fn derive_model(input: TokenStream) -> TokenStream {
    mool_macros_impl::model::derive_model(input.into(), quote::quote!(::vyuh::db)).into()
}

#[proc_macro_derive(Filterable, attributes(filter, db))]
pub fn derive_filterable(input: TokenStream) -> TokenStream {
    mool_macros_impl::filterable::derive_filterable(input.into(), quote::quote!(::vyuh::db)).into()
}

#[proc_macro_derive(SortKey, attributes(sort, db))]
pub fn derive_sort_key(input: TokenStream) -> TokenStream {
    mool_macros_impl::sort_key::derive_sort_key(input.into(), quote::quote!(::vyuh::db)).into()
}

/// Derives a SQL-backed enum mapping through Vyuh's database facade.
///
/// Generated implementations resolve through `vyuh::db`, so an application
/// does not need a direct Mool dependency.
#[proc_macro_derive(SqlEnum, attributes(sql_enum, db))]
pub fn derive_sql_enum(input: TokenStream) -> TokenStream {
    mool_macros_impl::sql_enum::derive_sql_enum(input.into(), quote::quote!(::vyuh::db)).into()
}

/// Registers a cron emitter.
///
/// This macro is sugar over `vyuh::bundles::cron(handler, CronConf)`.
/// The handler returns `Data<T>` and the emitted data is submitted to signals by
/// default. `executor = "task"` submits it to a registered durable task with
/// input type `T`.
///
/// # Attributes
///
/// - `expr` - Cron expression (required): `"0 0 * * * *"` (every minute)
/// - `executor` - `"signal"` (default) or `"task"`
/// - `schedule` - stable task cursor name (task executor only)
/// - `start` - `"next"` (default) or `"immediately"` (task executor only)
///
/// # Examples
///
/// ```ignore
/// // Free function
/// #[cron(expr = "0 0 * * * *")]
/// async fn publish_daily(site: Site) -> Data<DailyTick> {
///     DailyTick.into()
/// }
///
/// // Method in impl block
/// impl SyncTasks {
///     #[cron(expr = "0 */5 * * * *")]
///     async fn publish_frequent(site: Site) -> Data<SyncTick> {
///         SyncTick.into()
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn cron(attr: TokenStream, item: TokenStream) -> TokenStream {
    cron::parse_cron(attr, item)
}

/// Registers a fixed-interval emitter.
///
/// This macro is sugar over `vyuh::bundles::periodic(handler, PeriodicConf)`.
/// The handler returns `Data<T>` and the emitted data is submitted to signals by
/// default. `executor = "task"` submits it to a registered durable task with
/// input type `T`.
///
/// # Attributes
///
/// - `secs` - Interval in seconds (optional)
/// - `millis` - Interval in milliseconds (optional)
/// - `executor` - `"signal"` (default) or `"task"`
/// - `schedule` - stable task cursor name (task executor only)
/// - `start` - `"next"` (default) or `"immediately"` (task executor only)
///
/// At least one of `secs` or `millis` must be specified. Both can be used together.
///
/// # Examples
///
/// ```ignore
/// // Free function - runs every 30 seconds
/// #[periodic(secs = 30)]
/// async fn publish_health(site: Site) -> Data<HealthTick> {
///     HealthTick.into()
/// }
///
/// // Method - runs every 500ms
/// impl Monitor {
///     #[periodic(millis = 500)]
///     async fn publish_metrics(site: Site) -> Data<MetricsTick> {
///         MetricsTick.into()
///     }
/// }
///
/// // Combined - runs every 1.5 seconds
/// #[periodic(secs = 1, millis = 500)]
/// async fn publish_queue_tick(site: Site) -> Data<QueueTick> {
///     QueueTick.into()
/// }
/// ```
#[proc_macro_attribute]
pub fn periodic(attr: TokenStream, item: TokenStream) -> TokenStream {
    periodic::parse_periodic(attr, item)
}

/// Registers a PostgreSQL LISTEN/NOTIFY emitter.
///
/// This macro is sugar over `vyuh::bundles::pgnotify(handler, PgNotifyConf)`.
/// The handler receives raw notification data with `Data<String>` and returns
/// `Data<T>` for signal dispatch.
///
/// # Attributes
///
/// - `channel` - PostgreSQL channel name (required): `"user_updates"`
/// - `debounce_millis` / `debounce_secs` - Optional debounce window.
/// - `debounce` - Optional mode: `"leading"`, `"trailing"`, or
///   `"leading_trailing"`. Defaults to `"trailing"` when a debounce window is
///   configured.
///
/// # Examples
///
/// ```ignore
/// // Free function
/// #[pgnotify(channel = "user_updates")]
/// async fn publish_user_update(Data(raw): Data<String>) -> Data<UserUpdate> {
///     serde_json::from_str::<UserUpdate>(&raw).unwrap().into()
/// }
///
/// // Debounced notifications. Runs immediately, then once more with the last
/// // payload if more notifications arrive within 250ms.
/// #[pgnotify(
///     channel = "user_updates",
///     debounce_millis = 250,
///     debounce = "leading_trailing"
/// )]
/// async fn publish_debounced_user_update(Data(raw): Data<String>) -> Data<UserUpdate> {
///     serde_json::from_str::<UserUpdate>(&raw).unwrap().into()
/// }
///
/// // Method in impl block
/// impl UserHandlers {
///     #[pgnotify(channel = "notifications")]
///     async fn publish_notification(Data(raw): Data<String>) -> Data<Notification> {
///         serde_json::from_str::<Notification>(&raw).unwrap().into()
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn pgnotify(attr: TokenStream, item: TokenStream) -> TokenStream {
    pgnotify::parse_pgnotify(attr, item)
}

/// Registers a function as a typed signal handler.
///
/// This macro is sugar over `vyuh::bundles::signal(handler, SignalConf)`.
/// Annotated functions are registered for the data type extracted with
/// `Data<T>`. Signals are fire-and-forget in-process notifications; they do
/// not guarantee delivery, ordering, retries, durability, or handler completion.
///
/// # Examples
///
/// ```ignore
/// // Free function
/// #[signal]
/// async fn index_note_change(Data(event): Data<NoteChanged>) {
///     // handle typed signal
/// }
///
/// // Site can be extracted before the data.
/// #[signal]
/// async fn audit_note_change(site: Site, Data(event): Data<NoteChanged>) {
///     // use site plus typed data
/// }
/// ```
#[proc_macro_attribute]
pub fn signal(attr: TokenStream, item: TokenStream) -> TokenStream {
    signal::parse_signal(attr, item)
}

/// Registers a function as a durable task handler.
///
/// This macro is sugar over `vyuh::bundles::task(handler, TaskDefinition)`.
/// Task handlers accept `Data<T>` as their submitted data argument and return
/// `()`, `Result<(), Error>`, `TaskState`, or `Result<TaskState, Error>`.
///
/// # Attributes
///
/// - `name` - Optional task name (defaults to function name)
/// - `lane` - Optional static `TaskLane` descriptor (defaults to `default`)
/// - `idempotency` - Optional static `TaskIdempotency<T>` key rule
///
/// # Examples
///
/// ```ignore
/// // Free function with default name
/// #[task]
/// async fn send_email(Data(input): Data<EmailData>) -> Result<(), Error> {
///     deliver(input).await?;
///     Ok(())
/// }
///
/// // Method with custom name
/// impl TaskHandlers {
///     #[task(name = "custom_task_name", lane = EMAIL)]
///     async fn process_order(site: Site, Data(input): Data<Order>) -> Result<TaskState, Error> {
///         // process order
///         Ok(TaskState::complete())
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    task::parse_task(attr, item)
}

/// Builds an isolated `TestSite` around an async integration-test body.
///
/// This is syntax sugar over `vyuh::testing::test_site` and
/// `TestSite::builder(...).without_migrations()`.
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    test::parse_test(attr, item)
}

// #[proc_macro_attribute]
// pub fn fnspec(attr: TokenStream, item: TokenStream) -> TokenStream {
//     fnspec::parse_fnspec_input(attr, item, "fnspec")
// }

#[proc_macro_attribute]
pub fn openapi(attr: TokenStream, item: TokenStream) -> TokenStream {
    openapi::parse_openapi(attr, item)
}

#[proc_macro_attribute]
pub fn service(attr: TokenStream, item: TokenStream) -> TokenStream {
    service::parse_service(attr, item)
}

#[proc_macro_attribute]
pub fn asset_dir(attr: TokenStream, item: TokenStream) -> TokenStream {
    assets::parse_asset_dir(attr, item)
}

#[proc_macro_attribute]
pub fn migrations(attr: TokenStream, item: TokenStream) -> TokenStream {
    migrations::parse_migrations(attr, item)
}

#[proc_macro_attribute]
pub fn schema(attr: TokenStream, item: TokenStream) -> TokenStream {
    schema::parse_schema(attr, item)
}

#[proc_macro_attribute]
pub fn url_info(attr: TokenStream, item: TokenStream) -> TokenStream {
    url_info::parse_url_info(attr, item)
}
