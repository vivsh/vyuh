use schemars::JsonSchema;
use vyuh::{bundles, prelude::*, tasks::TaskDefinition};

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
struct Job;

async fn invalid_task(_: Data<Job>) -> Data<String> {
    Data::new("result".to_string())
}

fn main() {
    let _ = bundles::task(invalid_task, TaskDefinition::new("invalid_task"));
}
