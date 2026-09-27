use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;

fn site(_: Site, _: Data<Job>) {}
fn service(_: ServiceRef<String>, _: Data<Job>) {}
fn identity(_: TaskId, _: Data<Job>) {}
fn operation(_: vyuh::OperationId, _: Data<Job>) {}
fn main() {
 let _ = bundles::flow(site, TaskDefinition::new("site"));
 let _ = bundles::flow(service, TaskDefinition::new("service"));
 let _ = bundles::flow(identity, TaskDefinition::new("id"));
 let _ = bundles::flow(operation, TaskDefinition::new("operation"));
}
