use vyuh::{bundles, pravah, prelude::*};
use vyuh::tasks::{PravahDispatcher, WorkRequest};
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Job(u32);
type Root = pravah::Flow<Job>;
struct Dispatcher;
impl PravahDispatcher for Dispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> { Ok(Self) }
    fn fetch(&self, _: &pravah::Fetch) -> Result<WorkRequest, FlowError> { Err(FlowError::MissingDispatcher) }
}
#[bundles::flow(dispatch = Dispatcher)]
fn qualified(_: vyuh::PartialSite, root: Root) -> vyuh::pravah::Flow<u32> {
    root.map(|job: Job| job.0)
}
fn opaque(root: Root) -> impl IntoFlow<Job, Dispatcher> { qualified_root(root) }
fn qualified_root(root: Root) -> pravah::Flow<u32> { root.map(|job: Job| job.0) }
fn main() {
    let _ = bundles::bundle! { qualified };
    let _ = bundles::flow(opaque, FlowConf::new("opaque").dispatch::<Dispatcher>());
    let _ = bundles::flow(|| pravah::compile(qualified_root).map_err(|e| FlowError::fail(e.to_string())), FlowConf::new("compiled"));
}
