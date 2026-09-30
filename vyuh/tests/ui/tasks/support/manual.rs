use vyuh::prelude::*;
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
pub struct Job;
pub struct Manual;
impl Flow for Manual {
    type Input = Job;
    type Output = ();
    type Checkpoint = ();
    type Resume = ();
    fn advance(&self, _: TaskId, _: Data<Job>, _: Continuation<(), ()>) -> Result<FlowState, FlowError> {
        Ok(FlowState::complete(()))
    }
}
