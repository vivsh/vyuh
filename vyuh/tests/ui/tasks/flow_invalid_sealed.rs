use vyuh::prelude::*;
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
struct Job;

struct Bypass;
impl Flow for Bypass {
 type Input = Job;
 type Output = ();
 type Checkpoint = ();
 type Resume = ();
 async fn advance(&self, _: TaskId, _: Data<Job>, _: Continuation<(), ()>) -> Result<FlowState, FlowError> {
  Ok(FlowState::complete(()))
 }
}
fn main() {}
