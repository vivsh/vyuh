use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;
#[bundles::work_batch]
async fn handler(_: Continuation<()>, _: Data<Batch<Job>>) {}
fn main() {}
