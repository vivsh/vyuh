use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;
#[bundles::work]
async fn handler(_: Data<Job>) -> FlowState { FlowState::complete(()) }
#[bundles::work_batch]
async fn batch(_: Data<Batch<Job>>) -> FlowState { FlowState::complete(()) }
fn main() {}
