#[path = "support/manual.rs"]
mod support;
use vyuh::{bundles, prelude::*, tasks::{PravahEffects, WorkRequest}};
struct Effects;
impl PravahEffects for Effects {
    fn build(_: &PartialSite) -> Result<Self, FlowError> { Ok(Self) }
    fn fetch(&self, _: &vyuh::pravah::Fetch) -> Result<WorkRequest, FlowError> { Err(FlowError::MissingEffects) }
}
fn main() {
    let _ = bundles::flow(|| support::Manual, FlowConf::new("manual").effects::<Effects>());
}
