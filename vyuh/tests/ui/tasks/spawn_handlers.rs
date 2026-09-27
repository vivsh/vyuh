use schemars::JsonSchema;
use vyuh::{
    bundles,
    prelude::*,
    tasks::{TaskDefinition, TaskOptions},
};

#[derive(Deserialize, JsonSchema, Serialize)]
struct Parent;

#[derive(Deserialize, JsonSchema, Serialize)]
struct Child;

#[bundles::flow]
fn parent(_: Data<Parent>) -> Result<FlowState, FlowError> {
    Ok(FlowState::spawn(Child, ())?)
}

fn direct(_: Data<Parent>) -> Result<FlowState, FlowError> {
    Ok(FlowState::spawn_with(Child, (), TaskOptions::new())?)
}

fn main() {
    let _ = bundles::bundle! { parent };
    let _ = bundles::flow(direct, TaskDefinition::new("direct"));
}
