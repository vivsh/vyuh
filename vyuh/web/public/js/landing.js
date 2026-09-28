const examples = {
  routes: {
    file: "src/routes/orders.rs",
    eyebrow: "Routes",
    title: "Handler-first APIs without hiding the Rust.",
    copy: "Keep validation, permissions, and typed request handling visible where the behavior actually happens.",
    points: [
      "Less boilerplate, more clarity",
      "Compiler-verified request handling",
      "Built for long-term maintainability",
    ],
    code: `use vyuh::prelude::*;

#[derive(Serialize, Deserialize, JsonSchema, Validate)]
struct CreateOrder {
    #[validate(email)]
    customer_email: String,
    amount: i64,
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct OrderOut {
    id: i64,
    amount: i64,
}

#[bundles::route(path = "/orders", method = "POST")]
async fn create(
    site: Site,
    Valid(Data(input)): Valid<Data<CreateOrder>>,
) -> Result<Json<OrderOut>, Error> {
    let db = site.db();
    let id = Orders::insert(db, &input).await?;
    Ok(Json(OrderOut { id, amount: input.amount }))
}`,
  },
  tasks: {
    file: "src/tasks/receipts.rs",
    eyebrow: "Tasks",
    title: "Move background work into a first-class runtime path.",
    copy: "Tasks run with application context, explicit inputs, and the same dependency model as the web surface.",
    points: [
      "Typed payloads for queued work",
      "Shared app state without globals",
      "Retry and schedule behavior stays visible",
    ],
    code: `use vyuh::prelude::*;

#[derive(Serialize, Deserialize, JsonSchema, Validate)]
struct ReceiptJob {
    #[validate(email)]
    to: String,
    order_id: i64,
}

#[bundles::work(name = "send_receipt")]
async fn send_receipt(
    site: Site,
    Valid(Data(input)): Valid<Data<ReceiptJob>>,
) -> Result<(), WorkError> {
    let mail = site.service::<Mailer>()?;
    mail.send_receipt(input.order_id, &input.to).await?;
    Ok(())
}`,
  },
  signals: {
    file: "src/signals/orders.rs",
    eyebrow: "Signals",
    title: "Use typed signals for domain events and lifecycle hooks.",
    copy: "Signals keep local reactions decoupled without turning your app into a hidden event maze.",
    points: [
      "Explicit event contracts",
      "Multiple listeners without request coupling",
      "Good fit for audit, indexing, and follow-up work",
    ],
    code: `use vyuh::prelude::*;

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
struct OrderCreated {
    id: i64,
    total: i64,
}

#[bundles::signal]
async fn index_order(
    site: Site,
    Data(event): Data<OrderCreated>,
) -> Result<(), Error> {
    let search = site.service::<SearchIndex>()?;
    search.upsert_order(event.id, event.total).await?;
    Ok(())
}`,
  },
  emitters: {
    file: "src/emitters/reports.rs",
    eyebrow: "Emitters",
    title: "Let schedules emit typed events into the same runtime.",
    copy: "Cron, periodic, and database notifications produce typed data that feeds signal handlers.",
    points: [
      "One shape for emitted data",
      "Cron, periodic, and PgNotify sources",
      "Traceable integration points",
    ],
    code: `use vyuh::prelude::*;

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
struct ReportDue {
    tenant: String,
    count: u64,
}

#[bundles::cron(expr = "0 0 6 * * *")]
async fn daily_report(
    IterCount(count): IterCount,
) -> Data<ReportDue> {
    Data::new(ReportDue {
        tenant: "default".into(),
        count,
    })
}`,
  },
  commands: {
    file: "src/commands/reindex.rs",
    eyebrow: "Commands",
    title: "Give CLI commands the same app context as production paths.",
    copy: "Commands are operational entry points, not one-off scripts that reimplement setup and wiring.",
    points: [
      "Database and config already loaded",
      "Useful for migrations and maintenance",
      "Easy to document and discover",
    ],
    code: `use vyuh::{commands::CommandConf, prelude::*};

#[derive(Serialize, Deserialize, JsonSchema, Validate)]
struct Reindex {
    #[validate(min_length = 1)]
    tenant: String,
}

async fn reindex(
    site: Site,
    Valid(Data(args)): Valid<Data<Reindex>>,
) -> Result<(), Error> {
    let search = site.service::<SearchIndex>()?;
    search.rebuild(args.tenant).await?;
    Ok(())
}

fn command_bundle() -> vyuh::bundles::Bundle {
    bundles::bundle([bundles::command(
        reindex,
        CommandConf::new("search:reindex"),
    )])
}`,
  },
  channels: {
    file: "src/channels/events.rs",
    eyebrow: "Channels",
    title: "Stream typed events to clients without a second pub/sub model.",
    copy: "Channels expose selected signal payloads over WebSocket, SSE, or long polling.",
    points: [
      "One signal publish path",
      "Transport negotiation at the route edge",
      "Server-side delivery filters",
    ],
    code: `use vyuh::prelude::*;

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
struct OrderUpdated {
    order_id: i64,
    tenant: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Validate)]
struct EventsQuery {
    #[validate(min_length = 1)]
    tenant: String,
}

#[bundles::route(path = "/events")]
async fn events(
    Valid(Data(query)): Valid<Data<EventsQuery>>,
    sub: Subscriber,
    channels: Channels,
) -> Result<ChannelResponse, Error> {
    let tenant = query.tenant.clone();
    let stream = channels
        .user(UserKey::new(query.tenant.clone())?)
        .deliver_if::<OrderUpdated, _>(move |event| event.tenant == tenant);
    sub.attach(stream).allow(WS | SSE | POLL).await
}`,
  },
  services: {
    file: "src/services/search.rs",
    eyebrow: "Services",
    title: "Run site-lifetime services beside the web application.",
    copy: "Services are durable runtime components for shared clients, caches, coordinators, and workers.",
    points: [
      "Clear startup and shutdown behavior",
      "Shared runtime and observability",
      "No separate mini-framework for workers",
    ],
    code: `use vyuh::prelude::*;
use vyuh::services::{ServiceInstance, ServiceRef};

#[derive(Default)]
struct SearchIndex;

impl vyuh::services::Service for SearchIndex {}

#[bundles::service]
async fn search_index() -> ServiceInstance<SearchIndex> {
    SearchIndex::default().into()
}

#[bundles::route(path = "/search")]
async fn search(
    index: ServiceRef<SearchIndex>,
) -> Json<SearchStats> {
    Json(index.stats().await)
}`,
  },
};

const snippet = document.querySelector("#code-snippet");
const file = document.querySelector("#code-file");
const eyebrow = document.querySelector("#code-eyebrow");
const title = document.querySelector("#code-title");
const copy = document.querySelector("#code-copy");
const pointA = document.querySelector("#code-point-a");
const pointB = document.querySelector("#code-point-b");
const pointC = document.querySelector("#code-point-c");
const tabs = document.querySelectorAll(".landing-code-tab");
const consoleControls = document.querySelectorAll(".landing-console-control");
const consoleSlides = document.querySelectorAll(".landing-console-slide");
const consoleInference = document.querySelector("#console-inference");

function setExample(key) {
  const example = examples[key];
  snippet.textContent = example.code;
  file.textContent = example.file;
  eyebrow.textContent = example.eyebrow;
  title.textContent = example.title;
  copy.textContent = example.copy;
  pointA.textContent = example.points[0];
  pointB.textContent = example.points[1];
  pointC.textContent = example.points[2];
  tabs.forEach((tab) => {
    tab.classList.toggle("is-active", tab.dataset.key === key);
  });
}

tabs.forEach((tab) => {
  tab.addEventListener("click", () => setExample(tab.dataset.key));
});

function setConsoleSlide(index) {
  const slideCount = consoleSlides.length;
  if (!slideCount) {
    return;
  }
  const selectedIndex = Math.max(0, Math.min(index, slideCount - 1));
  consoleSlides.forEach((slide, slideIndex) => {
    const active = slideIndex === selectedIndex;
    slide.classList.toggle("is-active", active);
    if (active && consoleInference) {
      consoleInference.textContent = slide.dataset.inference;
    }
  });
  consoleControls.forEach((control, controlIndex) => {
    const active = controlIndex === selectedIndex;
    control.classList.toggle("is-active", active);
    control.setAttribute("aria-current", active ? "true" : "false");
  });
}

consoleControls.forEach((control) => {
  control.addEventListener("click", () => {
    const index = Number.parseInt(control.dataset.consoleSlide, 10);
    if (Number.isFinite(index)) {
      setConsoleSlide(index);
    }
  });
});

setExample("routes");
setConsoleSlide(0);
