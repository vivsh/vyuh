use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;
#[bundles::work]
async fn handler(_: Continuation<(), String>, _: Data<Job>) -> Result<WorkState<String>, WorkError> {
    Ok(WorkState::suspend(())?)
}
fn main() { let _ = bundles::work(handler, TaskDefinition::new("work")); }
