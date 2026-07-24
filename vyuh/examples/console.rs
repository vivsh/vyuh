//! Console inspection demo.
//!
//! Starts a small site with routes, tasks, signals, commands, and cron emitters
//! so the built-in console has representative operations to inspect.

use vyuh::{commands::CommandConf, console::ConsoleConf, prelude::*};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct HealthOut {
    status: String,
    service: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct CreateOrder {
    customer: String,
    amount: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct OrderOut {
    id: i64,
    customer: String,
    amount: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct ReceiptJob {
    order_id: i64,
    email: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct ProjectionJob {
    name: String,
    full: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct ProjectionOut {
    name: String,
    records: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct SignupSignal {
    user_id: i64,
    source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct InvoiceSignal {
    invoice_id: i64,
    amount: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct ConsoleTickSignal {
    tick: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct PrintTickJob {
    tick: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct TickPrintOut {
    tick: usize,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct SeedArgs {
    count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct ReportArgs {
    section: String,
    verbose: bool,
}

#[bundles::route(path = "/health")]
async fn health() -> Json<HealthOut> {
    Json(HealthOut {
        status: "ok".to_string(),
        service: "console-demo".to_string(),
    })
}

#[bundles::route(path = "/orders", method = "POST")]
async fn create_order(site: Site, Json(order): Json<CreateOrder>) -> Result<Json<OrderOut>, Error> {
    let created = OrderOut {
        id: 1001,
        customer: order.customer,
        amount: order.amount,
    };
    site.tasks()
        .submit(ReceiptJob {
            order_id: created.id,
            email: "buyer@example.com".to_string(),
        })
        .await
        .map_err(Error::other)?;
    site.signals()
        .emit(InvoiceSignal {
            invoice_id: created.id,
            amount: created.amount,
        })
        .map_err(Error::other)?;
    Ok(Json(created))
}

#[bundles::task(name = "send_receipt")]
async fn send_receipt(Data(job): Data<ReceiptJob>) {
    println!("send receipt for order {} to {}", job.order_id, job.email);
}

#[bundles::task(name = "rebuild_projection")]
async fn rebuild_projection(Data(job): Data<ProjectionJob>) -> Result<Data<ProjectionOut>, Error> {
    println!("rebuild projection '{}' full={}", job.name, job.full);
    Ok(Data::new(ProjectionOut {
        name: job.name.clone(),
        records: if job.full { 250 } else { 25 },
    }))
}

#[bundles::task(name = "print_console_tick")]
async fn print_console_tick(Data(job): Data<PrintTickJob>) -> Data<TickPrintOut> {
    println!("console periodic task fired at tick {}", job.tick);
    Data::new(TickPrintOut {
        tick: job.tick,
        message: format!("printed tick {}", job.tick),
    })
}

#[bundles::signal]
async fn audit_signup(Data(event): Data<SignupSignal>) {
    println!(
        "signup audit: user={} source={}",
        event.user_id, event.source
    );
}

#[bundles::signal]
async fn submit_tick_task(site: Site, Data(event): Data<ConsoleTickSignal>) -> Result<(), Error> {
    site.tasks()
        .submit(PrintTickJob { tick: event.tick })
        .await
        .map_err(Error::other)?;
    Ok(())
}

#[bundles::signal]
async fn audit_invoice(Data(event): Data<InvoiceSignal>) {
    println!(
        "invoice audit: invoice={} amount={}",
        event.invoice_id, event.amount
    );
}

#[bundles::cron(expr = "0/15 * * * * * *")]
async fn signup_tick(
    vyuh::emitters::IterCount(tick): vyuh::emitters::IterCount,
) -> Data<SignupSignal> {
    Data::new(SignupSignal {
        user_id: tick as i64,
        source: "signup-cron".to_string(),
    })
}

#[bundles::cron(expr = "5/15 * * * * * *")]
async fn invoice_tick(
    vyuh::emitters::IterCount(tick): vyuh::emitters::IterCount,
) -> Data<InvoiceSignal> {
    Data::new(InvoiceSignal {
        invoice_id: tick as i64,
        amount: 100 + tick as i64,
    })
}

// Every five seconds this emitter emits a signal. The signal handler above
// submits `print_console_tick`, so task records keep appearing in the console.
#[bundles::periodic(millis = 5000)]
async fn console_tick(
    vyuh::emitters::IterCount(tick): vyuh::emitters::IterCount,
) -> Data<ConsoleTickSignal> {
    Data::new(ConsoleTickSignal { tick })
}

async fn seed_demo(site: Site, Data(args): Data<SeedArgs>) -> Result<(), Error> {
    site.tasks()
        .submit(ReceiptJob {
            order_id: args.count,
            email: "seed@example.com".to_string(),
        })
        .await
        .map_err(Error::other)?;
    site.tasks()
        .submit(ProjectionJob {
            name: "orders".to_string(),
            full: true,
        })
        .await
        .map_err(Error::other)?;
    Ok(())
}

async fn print_report(site: Site, Data(args): Data<ReportArgs>) -> Result<(), Error> {
    site.signals()
        .emit(SignupSignal {
            user_id: 42,
            source: args.section.clone(),
        })
        .map_err(Error::other)?;
    println!("report section='{}' verbose={}", args.section, args.verbose);
    Ok(())
}

fn app_bundle() -> vyuh::bundles::Bundle {
    bundles::bundle! {
        health,
        create_order,
        send_receipt,
        rebuild_projection,
        print_console_tick,
        audit_signup,
        submit_tick_task,
        audit_invoice,
        signup_tick,
        invoice_tick,
        console_tick,
    }
    .merge(command_bundle())
}

fn command_bundle() -> vyuh::bundles::Bundle {
    bundles::bundle([
        bundles::command(
            seed_demo,
            CommandConf::new("demo:seed").description("Submit demo task records."),
        ),
        bundles::command(
            print_report,
            CommandConf::new("demo:report").description("Emit a demo report signal."),
        ),
    ])
}

#[tokio::main]
async fn main() -> Result<(), SiteError> {
    let conf = SiteConf::default()
        .port(8080)
        .console(ConsoleConf::default());
    let site = Site::build(conf, app_bundle()).await?;
    println!("Console demo running on http://localhost:8080");
    site.start().await?;
    Ok(())
}
