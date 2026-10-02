use vyuh::{bundles, prelude::*, tasks::{PravahDispatcher, WorkRequest}};
struct Dispatcher;
impl PravahDispatcher for Dispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> { Ok(Self) }
    fn fetch(&self, _: &vyuh::pravah::Fetch) -> Result<WorkRequest, FlowError> {
        Err(FlowError::MissingDispatcher)
    }
}
#[bundles::flow(effects = Dispatcher)]
fn old_attribute(root: vyuh::pravah::Flow<u32>) -> vyuh::pravah::Flow<u32> { root }
fn main() {}
