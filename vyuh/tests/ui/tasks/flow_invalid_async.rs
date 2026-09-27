use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;
#[bundles::flow]
async fn handler(_: Data<Job>) {}
fn future(_: Data<Job>) -> impl std::future::Future<Output=()> { async {} }
fn main() { let _ = bundles::flow(future, TaskDefinition::new("future")); }
