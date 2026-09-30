use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;

type Input = Data<Job>;
type State = FlowState;
struct Manual;
impl Flow for Manual {
 type Input = Job;
 type Output = ();
 type Checkpoint = ();
 type Resume = ();
 fn advance(&self, _: TaskId, _: Input, _: Continuation<(), ()>) -> Result<State, FlowError> {
  Ok(State::complete(()))
 }
}
type Definition = Manual;
#[bundles::flow(name = "qualified", lane = DEFAULT_TASK_LANE)]
fn qualified(_: vyuh::PartialSite) -> Result<Definition, FlowError> { Ok(Manual) }
fn unit() -> Manual { Manual }
fn result_unit() -> Result<Manual, FlowError> { Ok(Manual) }
fn state() -> impl Flow<Input = Job> { Manual }
struct Methods;
impl Methods { fn flow() -> Result<Manual, FlowError> { Ok(Manual) } }
fn main() {
    let _ = bundles::bundle! { qualified };
    let _ = bundles::flow(unit, FlowConf::new("unit"));
    let _ = bundles::flow(result_unit, FlowConf::new("result_unit"));
    let _ = bundles::flow(state, FlowConf::new("state"));
    let _ = bundles::flow(Methods::flow, FlowConf::new("method"));
    let _ = bundles::flow(|| Manual, FlowConf::new("closure"));
    let _ = bundles::work(|_: Input| async {}, TaskDefinition::new("work"));
}
