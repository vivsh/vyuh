#[path = "support/manual.rs"]
mod support;
use vyuh::{bundles, prelude::*, tasks::{PravahDispatcher, WorkRequest}};
struct Dispatcher;
impl PravahDispatcher for Dispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> { Ok(Self) }
    fn fetch(&self, _: &vyuh::pravah::Fetch) -> Result<WorkRequest, FlowError> { Err(FlowError::MissingDispatcher) }
}
fn main() {
    let _ = bundles::flow(|| support::Manual, FlowConf::new("manual").dispatch::<Dispatcher>());
}
