use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;
#[bundles::task_batch]
async fn handler(_: Continuation<()>, _: Data<Batch<Job>>) {}
fn main() {}
