use vyuh::{bundles, prelude::*, tasks::TaskDefinition};

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Input;

async fn invalid(_: Data<Batch<Input>>) -> Result<FlowState, FlowError> {
    Ok(FlowState::all(Vec::<Input>::new(), ())?)
}

fn main() {
    let _ = bundles::task_batch(invalid, TaskDefinition::new("invalid"));
}
