use vyuh::{bundles, prelude::*, tasks::TaskDefinition};
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;

fn main() {
 let _ = TaskState::sleep((), std::time::Duration::ZERO);
 let _ = TaskState::spawn(Job, ());
 let _ = TaskState::spawn_with(Job, (), vyuh::tasks::TaskOptions::new());
}
