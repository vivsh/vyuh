use schemars::JsonSchema;
use vyuh::{bundles, prelude::*, tasks::TaskDefinition};

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
struct Job;

type JobBatch = Batch<Job>;

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
struct QualifiedJob;

#[bundles::work_batch]
async fn macro_batch(_: Data<JobBatch>) -> Result<Batch<WorkState>, WorkError> {
    Ok(Batch::new(vec![WorkState::complete(())]))
}

#[bundles::work_batch(name = "qualified_batch")]
async fn qualified_batch(_: Data<vyuh::tasks::Batch<QualifiedJob>>) {}

async fn direct_batch(_: Data<Batch<Job>>) -> Result<(), WorkError> {
    Ok(())
}

fn main() {
    let _ = bundles::bundle! { macro_batch, qualified_batch };
    let _ = bundles::work_batch(direct_batch, TaskDefinition::new("direct_batch"));
}
