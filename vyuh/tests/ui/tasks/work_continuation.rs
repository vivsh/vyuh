use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;
#[bundles::task]
async fn handler(_: Continuation<(), String>, _: Data<Job>) -> Result<TaskState<String>, TaskError> {
    Ok(TaskState::suspend(())?)
}
fn main() { let _ = bundles::task(handler, TaskDefinition::new("work")); }
