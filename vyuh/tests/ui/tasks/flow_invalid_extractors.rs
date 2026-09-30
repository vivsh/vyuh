use vyuh::{bundles, prelude::*};
#[path = "support/manual.rs"] mod support;
use support::{Job, Manual};

fn site(_: Site) -> Manual { Manual }
fn service(_: ServiceRef<String>) -> Manual { Manual }
fn identity(_: TaskId) -> Manual { Manual }
fn operation(_: vyuh::OperationId) -> Manual { Manual }
fn input(_: Data<Job>) -> Manual { Manual }
fn continuation(_: Continuation<(), ()>) -> Manual { Manual }
fn main() {
 let _ = bundles::flow(site, FlowConf::new("site"));
 let _ = bundles::flow(service, FlowConf::new("service"));
 let _ = bundles::flow(identity, FlowConf::new("id"));
 let _ = bundles::flow(operation, FlowConf::new("operation"));
 let _ = bundles::flow(input, FlowConf::new("input"));
 let _ = bundles::flow(continuation, FlowConf::new("continuation"));
}
