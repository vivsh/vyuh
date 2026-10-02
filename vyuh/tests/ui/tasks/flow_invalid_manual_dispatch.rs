use vyuh::prelude::*;
#[path = "support/manual.rs"] mod support;
struct Dispatcher;
#[cfg(feature = "pravah")]
impl vyuh::tasks::PravahDispatcher for Dispatcher {
 fn build(_: &PartialSite) -> Result<Self, FlowError> { Ok(Self) }
 fn fetch(&self, _: &vyuh::pravah::Fetch) -> Result<vyuh::tasks::WorkRequest, FlowError> {
  Err(FlowError::MissingDispatcher)
 }
}
// Manual definitions cannot bind a graph-specific dispatcher.
fn main() {
    let _ = bundles::flow(|| support::Manual, FlowConf::new("manual").dispatch::<Dispatcher>());
}
