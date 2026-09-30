#[path = "support/manual.rs"]
mod support;
use vyuh::{bundles, prelude::*};
fn handler(_: Data<support::Job>, _: Continuation<()>) -> support::Manual { support::Manual }
fn main() { let _ = bundles::flow(handler, FlowConf::new("bad")); }
