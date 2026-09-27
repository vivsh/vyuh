use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;
#[bundles::task]
async fn handler(_: Data<Job>) -> FlowState { FlowState::complete(()) }
#[bundles::task_batch]
async fn batch(_: Data<Batch<Job>>) -> FlowState { FlowState::complete(()) }
fn main() {}
