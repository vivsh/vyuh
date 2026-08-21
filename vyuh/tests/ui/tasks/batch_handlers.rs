use schemars::JsonSchema;
use vyuh::{bundles, prelude::*, tasks::TaskDefinition};

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
struct Job;

type JobBatch = Batch<Job>;

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
struct QualifiedJob;

#[bundles::task_batch]
async fn macro_batch(_: Data<JobBatch>) -> Batch<TaskState> {
    Batch::new(vec![TaskState::complete()])
}

#[bundles::task_batch(name = "qualified_batch")]
async fn qualified_batch(_: Data<vyuh::tasks::Batch<QualifiedJob>>) {}

async fn direct_batch(_: Data<Batch<Job>>) -> Result<(), Error> {
    Ok(())
}

fn main() {
    let _ = bundles::bundle! { macro_batch, qualified_batch };
    let _ = bundles::task_batch(direct_batch, TaskDefinition::new("direct_batch"));
}
