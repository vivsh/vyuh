use vyuh::{bundles, pravah, prelude::*};
use vyuh::tasks::{PravahEffects, WorkRequest};
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Job(u32);
type Root = pravah::Flow<Job>;
struct Effects;
impl PravahEffects for Effects {
    fn build(_: &PartialSite) -> Result<Self, FlowError> { Ok(Self) }
    fn fetch(&self, _: &pravah::Fetch) -> Result<WorkRequest, FlowError> { Err(FlowError::MissingEffects) }
}
#[bundles::flow(effects = Effects)]
fn qualified(_: vyuh::PartialSite, root: Root) -> vyuh::pravah::Flow<u32> {
    root.map(|job: Job| job.0)
}
fn opaque(root: Root) -> impl IntoFlow<Job, Effects> { qualified_root(root) }
fn qualified_root(root: Root) -> pravah::Flow<u32> { root.map(|job: Job| job.0) }
fn main() {
    let _ = bundles::bundle! { qualified };
    let _ = bundles::flow(opaque, FlowConf::new("opaque").effects::<Effects>());
    let _ = bundles::flow(|| pravah::compile(qualified_root).map_err(|e| FlowError::fail(e.to_string())), FlowConf::new("compiled"));
}
