use schemars::JsonSchema;
use vyuh::{
    bundles,
    prelude::*,
    tasks::TaskOptions,
};

#[derive(Deserialize, JsonSchema, Serialize)]
struct Parent;

#[derive(Deserialize, JsonSchema, Serialize)]
struct Child;

#[bundles::flow]
fn parent() -> Manual { Manual }

struct Manual;
impl Flow for Manual {
 type Input = Parent;
 type Output = ();
 type Checkpoint = ();
 type Resume = ();
 fn advance(&self, _: TaskId, _: Data<Parent>, _: Continuation<(), ()>) -> Result<FlowState, FlowError> {
    Ok(FlowState::spawn_with(Child, (), TaskOptions::new())?)
 }
}

fn main() {
    let _ = bundles::bundle! { parent };
    let _ = bundles::flow(parent, FlowConf::new("direct"));
}
