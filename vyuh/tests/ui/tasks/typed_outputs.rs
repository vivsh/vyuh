use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;
#[derive(Serialize)]
struct Output { value: u32 }
type Completion = TaskState<Output>;
#[bundles::task]
async fn direct(_: Data<Job>) -> Output { Output { value: 42 } }
#[bundles::task]
async fn fallible(_: Data<Job>) -> Result<Output, TaskError> { Ok(Output { value: 42 }) }
async fn state(_: Data<Job>) -> Result<Completion, TaskError> { Ok(TaskState::complete(Output { value: 42 })) }
fn flow(_: Data<Job>) -> Result<FlowState<Output>, FlowError> { Ok(FlowState::complete(Output { value: 42 })) }
#[bundles::task_batch]
async fn batch(_: Data<Batch<Job>>) -> Batch<Result<Completion, TaskError>> {
    vec![Ok(TaskState::complete(Output { value: 42 })), Err(TaskError::retry("later"))].into()
}
fn main() {
    let _ = bundles::task(direct, TaskDefinition::new("direct"));
    let _ = bundles::task(fallible, TaskDefinition::new("fallible"));
    let _ = bundles::task(state, TaskDefinition::new("state"));
    let _ = bundles::task(|_: Data<Job>| async { 42u32 }, TaskDefinition::new("closure"));
    let _ = bundles::flow(flow, TaskDefinition::new("flow"));
}
