use vyuh::{bundles, prelude::*};
#[bundles::flow]
fn handler() -> WorkState { WorkState::complete(()) }
fn main() { let _ = bundles::flow(|| 42u32, FlowConf::new("bad")); }
