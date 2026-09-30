use vyuh::prelude::*;
#[path = "support/manual.rs"] mod support;
struct Effects;
#[cfg(feature = "pravah")]
impl vyuh::tasks::PravahEffects for Effects {
 fn build(_: &PartialSite) -> Result<Self, FlowError> { Ok(Self) }
 fn fetch(&self, _: &vyuh::pravah::Fetch) -> Result<vyuh::tasks::WorkRequest, FlowError> {
  Err(FlowError::MissingEffects)
 }
}
// Manual definitions cannot bind a graph-specific effects policy.
fn main() {
    let _ = bundles::flow(|| support::Manual, FlowConf::new("manual").effects::<Effects>());
}
