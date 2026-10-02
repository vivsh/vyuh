use vyuh::{prelude::*, tasks::{PravahDispatcher, WorkRequest}};
struct Dispatcher;
impl PravahDispatcher for Dispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> { Ok(Self) }
    fn fetch(&self, _: &vyuh::pravah::Fetch) -> Result<WorkRequest, FlowError> {
        Err(FlowError::MissingDispatcher)
    }
}
fn main() {
    let _ = FlowConf::<u32>::new("legacy").effects::<Dispatcher>();
    let _ = FlowError::MissingEffects;
}
