use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;

fn main() {
 let _ = WorkState::sleep((), std::time::Duration::ZERO);
 let _ = WorkState::spawn(Job, ());
 let _ = WorkState::spawn_with(Job, (), vyuh::tasks::TaskOptions::new());
}
