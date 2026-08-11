/// Task handler patterns and suspend/resume workflow.
///
/// Covers:
///   1. Fire-and-forget                (no return)
///   2. Fallible fire-and-forget       (Result<(), Error>)
///   3. Method-based registration      (no #[bundles::task] macro)
///   4. Suspend/resume with enum state (Result<TaskState, Error>)
use schemars::JsonSchema;
use std::time::Duration;
use vyuh::prelude::*;
use vyuh::tasks::{TaskConf, TaskIdempotency, TaskLaneConf, TaskOptions, TaskRate, TaskRetry};

const EMAIL: TaskLane = TaskLane::new("email");

// ── Input types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct SendEmailJob {
    to: String,
    subject: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct ProcessingJob {
    data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct ApprovalRequest {
    document_id: i64,
    title: String,
    submitter: String,
}

// Resume payload sent by the approver.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ApprovalDecision {
    Approved { approver: String },
    Rejected { approver: String, reason: String },
}

// Internal state persisted while the task is suspended.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingApproval {
    document_id: i64,
    title: String,
}

// ── Handlers ─────────────────────────────────────────────────────────────────

// Pattern 1: Fire-and-forget — macro with explicit name.
#[bundles::task(
    name = "send_email",
    lane = EMAIL,
    idempotency = TaskIdempotency::new("send-email-v1", email_key)
)]
async fn send_email(input: Data<SendEmailJob>) {
    println!(
        "📧 Sending email to {} — subject: {}",
        input.to, input.subject
    );
}

// Pattern 2: Fallible — macro without explicit name (derives from fn name).
#[bundles::task]
async fn process_data(input: Data<ProcessingJob>) -> Result<(), Error> {
    println!("⚙️  Processing: {}", input.data);
    Ok(())
}

// Without the macro, register manually:
//   async fn process_data(input: Data<ProcessingJob>) -> Result<(), Error> { ... }
// Then pass to Site::build via a separate bundle:
//   let extra = bundles::bundle([bundles::task(
//       process_data,
//       tasks::TaskDefinition::new("process_data"),
//   )]);

fn email_key(job: &SendEmailJob) -> String {
    format!("welcome:{}", job.to)
}

// Pattern 4: Suspend/resume with typed continuation state and input.
#[bundles::task(name = "approve_document")]
async fn approve_document(
    continuation: Continuation<PendingApproval, ApprovalDecision>,
    input: Data<ApprovalRequest>,
) -> Result<TaskState, Error> {
    match continuation.resume() {
        // ── Resumed: approver has responded ──────────────────────────────
        Some(decision) => {
            match &decision {
                ApprovalDecision::Approved { approver } => {
                    println!("✅ '{}' approved by {}", input.title, approver);
                }
                ApprovalDecision::Rejected { approver, reason } => {
                    println!("❌ '{}' rejected by {} — {}", input.title, approver, reason);
                }
            }
            Ok(TaskState::complete())
        }

        // ── First run: suspend and wait ───────────────────────────────────
        None => {
            println!(
                "⏳ '{}' (id={}) by {} — waiting for approval",
                input.title, input.document_id, input.submitter
            );

            let state = PendingApproval {
                document_id: input.document_id,
                title: input.title.clone(),
            };
            Ok(TaskState::suspend(state)?)
        }
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Macro-annotated handlers go directly in bundle!
    let bundle = bundles::bundle! {
        send_email,
        process_data,
        approve_document,
    }
    .with_conf(bundles::conf().task_lane(TaskLaneConf::new(EMAIL, 2)));

    let task_conf = TaskConf::default()
        .concurrency(4)
        .batch_size(50)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 2))
        .lane(
            TaskLaneConf::new(EMAIL, 2)
                .retry(
                    TaskRetry::exponential(5, Duration::from_secs(2))
                        .max_delay(Duration::from_secs(60)),
                )
                .rate_limit(TaskRate::per_second(10).burst(5))
                .global_rate_limit(TaskRate::per_minute(120).burst(10))
                .idempotency_retention(Duration::from_secs(30 * 24 * 60 * 60)),
        );
    let conf = SiteConf::default().tasks(task_conf);
    let site = Site::build(conf, bundle).await.map_err(Error::other)?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await.map_err(Error::other)?;
    let tasks = site.tasks();

    // ── Fire-and-forget tasks ─────────────────────────────────────────────
    tasks
        .submit_many_with(
            [
                SendEmailJob {
                    to: "user@example.com".to_string(),
                    subject: "Hello from Vyuh".to_string(),
                },
                SendEmailJob {
                    to: "editor@example.com".to_string(),
                    subject: "A second batched email".to_string(),
                },
            ],
            TaskOptions::new(),
        )
        .await
        .map_err(Error::other)?;

    tasks
        .submit(ProcessingJob {
            data: "important payload".to_string(),
        })
        .await
        .map_err(Error::other)?;

    // ── Suspend/resume tasks ──────────────────────────────────────────────
    let doc1 = tasks
        .submit(ApprovalRequest {
            document_id: 101,
            title: "Q4 Budget Proposal".to_string(),
            submitter: "alice".to_string(),
        })
        .await
        .map_err(Error::other)?;

    let doc2 = tasks
        .submit(ApprovalRequest {
            document_id: 102,
            title: "New Hire Policy".to_string(),
            submitter: "bob".to_string(),
        })
        .await
        .map_err(Error::other)?;

    // Allow the task engine to run and suspend the approval tasks.
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    tasks
        .resume(
            doc1.id(),
            ApprovalDecision::Approved {
                approver: "carol".to_string(),
            },
        )
        .await
        .map_err(Error::other)?;

    tasks
        .resume(
            doc2.id(),
            ApprovalDecision::Rejected {
                approver: "carol".to_string(),
                reason: "Budget not aligned with targets".to_string(),
            },
        )
        .await
        .map_err(Error::other)?;

    // Allow resumed tasks to complete.
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    runtime.shutdown_and_wait().await;

    Ok(())
}
