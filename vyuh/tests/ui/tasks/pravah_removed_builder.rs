use vyuh::{prelude::*, tasks::{PravahDispatcher, WorkRequest}};
struct Dispatcher;
impl PravahDispatcher for Dispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> { Ok(Self) }
    fn agent(&self, _: &vyuh::pravah::AgentRequest) -> Result<WorkRequest, FlowError> {
        Err(FlowError::MissingDispatcher)
    }
}
fn main() {
    let _ = FlowConf::<u32>::new("legacy").effects::<Dispatcher>();
    let _ = FlowError::MissingEffects;
}
