use vyuh::prelude::*;
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;
struct Effects;
#[bundles::work(effects = Effects)]
async fn invalid(_: Data<Job>) {}
#[bundles::work_batch(effects = Effects)]
async fn invalid_batch(_: Data<Batch<Job>>) {}
fn main() {}
