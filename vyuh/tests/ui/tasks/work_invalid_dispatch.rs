use vyuh::prelude::*;
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;
struct Dispatcher;
#[bundles::work(dispatch = Dispatcher)]
async fn invalid(_: Data<Job>) {}
#[bundles::work_batch(dispatch = Dispatcher)]
async fn invalid_batch(_: Data<Batch<Job>>) {}
fn main() {}
