#[path = "support/manual.rs"]
mod support;
use vyuh::{bundles, prelude::*};
#[bundles::flow]
async fn handler() -> support::Manual { support::Manual }
fn future() -> impl std::future::Future<Output=support::Manual> { async { support::Manual } }
fn main() { let _ = bundles::flow(future, FlowConf::new("future")); }
