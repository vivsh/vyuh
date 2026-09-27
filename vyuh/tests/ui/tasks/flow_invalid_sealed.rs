use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;

struct Bypass;
impl vyuh::tasks::IntoFlowOutcomePart for Bypass {
 fn into_flow_state(self) -> FlowState { FlowState::complete(()) }
}
fn main() {}
