use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;
fn handler(_: Data<Job>, _: Continuation<()>) {}
fn main() { let _ = bundles::flow(handler, TaskDefinition::new("bad")); }
