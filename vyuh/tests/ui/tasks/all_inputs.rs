use vyuh::prelude::*;
use vyuh::tasks::TaskDefinition;

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Image(u32);
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Document(String);
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Job;

type Inputs = vyuh::tasks::FlowAllIn;
type Outputs = vyuh::tasks::FlowAllOut;
struct Manual;
impl Flow for Manual {
    type Input = Job;
    type Output = ();
    type Checkpoint = ();
    type Resume = Outputs;

    fn advance(&self, _: TaskId, _: Data<Job>, continuation: Continuation<(), Outputs>) -> Result<FlowState, FlowError> {
        if let (_, Some(results)) = continuation.into_parts() {
            let results = results?;
            let _: u32 = results.decode(0)??;
            let _: String = results.decode(1)??;
            return Ok(FlowState::complete(()));
        }
        Ok(FlowState::all((Image(1), Document("doc".into())), ())?)
    }
}

#[bundles::flow]
fn factory() -> impl Flow<Input = Job> { Manual }

#[bundles::work]
async fn image(input: Data<Image>) -> u32 { input.0.0 }

#[bundles::work]
async fn document(input: Data<Document>) -> String { input.0.0.clone() }

fn main() -> Result<(), TaskRuntimeError> {
    let _ = FlowState::<()>::all(vec![Image(1)], ())?;
    let _ = FlowState::<()>::all((Image(2),), ())?;
    let _ = FlowState::<()>::all((Image(3), Document("doc".into())), ())?;
    let mut inputs = Inputs::new().with(Image(4));
    inputs.push(Document("dynamic".into()));
    let _ = FlowState::<()>::all(inputs, ())?;
    let _ = bundles::bundle! { factory, image, document };
    let _ = bundles::flow(factory, FlowConf::new("factory"));
    let _ = bundles::work(image, TaskDefinition::new("image"));
    Ok(())
}
