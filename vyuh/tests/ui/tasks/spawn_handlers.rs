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

#[bundles::task]
async fn parent(_: Data<Parent>) -> Result<TaskState, Error> {
    Ok(TaskState::spawn(Child, ())?)
}

async fn direct(_: Data<Parent>) -> Result<TaskState, Error> {
    Ok(TaskState::spawn_with(Child, (), TaskOptions::new())?)
}

fn main() {
    let _ = bundles::bundle! { parent };
    let _ = bundles::task(direct, TaskDefinition::new("direct"));
}
