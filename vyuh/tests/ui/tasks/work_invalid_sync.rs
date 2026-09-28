use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;
#[bundles::work]
fn handler(_: Data<Job>) {}
fn main() {}
