use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;

type Input = Data<Job>;
type State = FlowState;
#[bundles::flow(name = "qualified", lane = DEFAULT_TASK_LANE)]
fn qualified(_: vyuh::tasks::Continuation<(), ()>, _: Input) -> Result<vyuh::tasks::FlowState, FlowError> {
    Ok(State::complete(()))
}
fn unit(_: Input) {}
fn result_unit(_: Input) -> Result<(), FlowError> { Ok(()) }
fn state(_: Input) -> State { State::complete(()) }
struct Methods;
impl Methods { fn flow(_: Input) -> Result<State, FlowError> { Ok(State::complete(())) } }
fn main() {
    let _ = bundles::bundle! { qualified };
    let _ = bundles::flow(unit, TaskDefinition::new("unit"));
    let _ = bundles::flow(result_unit, TaskDefinition::new("result_unit"));
    let _ = bundles::flow(state, TaskDefinition::new("state"));
    let _ = bundles::flow(Methods::flow, TaskDefinition::new("method"));
    let _ = bundles::flow(|_: Input| (), TaskDefinition::new("closure"));
    let _ = bundles::task(|_: Input| async {}, TaskDefinition::new("work"));
}
