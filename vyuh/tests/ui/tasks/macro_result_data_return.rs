use schemars::JsonSchema;
use vyuh::prelude::*;

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
struct Job;

#[bundles::task]
async fn invalid_task(_: Data<Job>) -> Result<Data<String>, Error> {
    Ok(Data::new("result".to_string()))
}

fn main() {
    let _ = bundles::bundle! { invalid_task };
}
