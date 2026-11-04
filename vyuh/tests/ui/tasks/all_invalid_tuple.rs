use vyuh::prelude::*;

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Job;

fn main() {
    let _ = FlowState::<()>::all(
        (Job, Job, Job, Job, Job, Job, Job, Job, Job, Job, Job, Job, Job, Job, Job, Job, Job),
        (),
    );
}
