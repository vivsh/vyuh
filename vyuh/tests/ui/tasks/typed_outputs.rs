use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;
#[derive(Serialize)]
struct Output { value: u32 }
type Completion = WorkState<Output>;
#[bundles::work]
async fn direct(_: Data<Job>) -> Output { Output { value: 42 } }
#[bundles::work]
async fn fallible(_: Data<Job>) -> Result<Output, WorkError> { Ok(Output { value: 42 }) }
async fn state(_: Data<Job>) -> Result<Completion, WorkError> { Ok(WorkState::complete(Output { value: 42 })) }
fn flow(_: Data<Job>) -> Result<FlowState<Output>, FlowError> { Ok(FlowState::complete(Output { value: 42 })) }
#[bundles::work_batch]
async fn batch(_: Data<Batch<Job>>) -> Batch<Result<Completion, WorkError>> {
    vec![Ok(WorkState::complete(Output { value: 42 })), Err(WorkError::retry("later"))].into()
}
fn main() {
    let _ = bundles::work(direct, TaskDefinition::new("direct"));
    let _ = bundles::work(fallible, TaskDefinition::new("fallible"));
    let _ = bundles::work(state, TaskDefinition::new("state"));
    let _ = bundles::work(|_: Data<Job>| async { 42u32 }, TaskDefinition::new("closure"));
    let _ = bundles::flow(flow, TaskDefinition::new("flow"));
}
