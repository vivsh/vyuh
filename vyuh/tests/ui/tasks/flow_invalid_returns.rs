use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;
#[bundles::flow]
fn handler(_: Data<Job>) -> WorkState { WorkState::complete(()) }
fn main() { let _ = bundles::flow(|_: Data<Job>| 42u32, TaskDefinition::new("bad")); }
