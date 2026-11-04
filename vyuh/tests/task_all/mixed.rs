use super::*;
use vyuh::tasks::{TaskFilter, TaskInfo, TaskLaneConf};

const GPU: TaskLane = TaskLane::new("mixed-gpu");
const IO: TaskLane = TaskLane::new("mixed-io");

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Image(u32);
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Document(u32);
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct TupleGroup(bool);
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct DynamicGroup {
    mode: u8,
    failed: bool,
}
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Unknown;
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Nested(u32);
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Batched(u32);
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct ExtendedGroup;
#[derive(Serialize, Deserialize)]
struct ImageResult {
    width: u32,
}
#[derive(Serialize, Deserialize)]
struct DocumentResult {
    content: String,
}
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Summary {
    image: u32,
    document: Option<String>,
}

#[bundles::work(lane = GPU)]
async fn mixed_image(input: Data<Image>) -> ImageResult {
    tokio::time::sleep(Duration::from_millis(20)).await;
    ImageResult {
        width: input.0.0 * 2,
    }
}

#[bundles::work(lane = IO)]
async fn mixed_document(input: Data<Document>) -> Result<DocumentResult, WorkError> {
    if input.0.0 == 0 {
        return Err(WorkError::fail("document unavailable"));
    }
    Ok(DocumentResult {
        content: format!("doc-{}", input.0.0),
    })
}

#[bundles::flow]
fn tuple_group() -> TupleFlow {
    TupleFlow
}
#[bundles::flow]
fn dynamic_group() -> DynamicFlow {
    DynamicFlow
}

#[bundles::flow(lane = IO)]
fn nested_group() -> NestedFlow {
    NestedFlow
}

#[bundles::work_batch(lane = GPU)]
async fn mixed_batch(input: Data<Batch<Batched>>) -> Batch<WorkState<u32>> {
    input
        .iter()
        .map(|item| WorkState::complete(item.0 * 2))
        .collect()
}

#[bundles::flow]
fn extended_group() -> ExtendedFlow {
    ExtendedFlow
}

struct NestedFlow;
impl Flow for NestedFlow {
    type Input = Nested;
    type Output = String;
    type Checkpoint = ();
    type Resume = ();

    fn advance(
        &self,
        _: TaskId,
        input: Data<Nested>,
        _: Continuation<(), ()>,
    ) -> Result<FlowState<String>, FlowError> {
        Ok(FlowState::complete(format!("nested-{}", input.0.0)))
    }
}

type ExtendedResults = (
    Result<String, TaskFailure>,
    Result<u32, TaskFailure>,
    Result<u32, TaskFailure>,
);
struct ExtendedFlow;
impl Flow for ExtendedFlow {
    type Input = ExtendedGroup;
    type Output = (String, u32, u32);
    type Checkpoint = ();
    type Resume = ExtendedResults;

    /// Mixes a registered Flow child with repeated Work-batch inputs without changing dispatch.
    fn advance(
        &self,
        _: TaskId,
        _: Data<ExtendedGroup>,
        continuation: Continuation<(), ExtendedResults>,
    ) -> Result<FlowState<Self::Output>, FlowError> {
        match continuation.into_parts() {
            (None, None) => Ok(FlowState::all((Nested(5), Batched(7), Batched(9)), ())?),
            (Some(()), Some(results)) => {
                let (nested, first, second) = results?;
                Ok(FlowState::complete((nested?, first?, second?)))
            }
            _ => Err(FlowError::fail("invalid extended continuation")),
        }
    }
}

type MixedResults = (
    Result<ImageResult, TaskFailure>,
    Result<DocumentResult, TaskFailure>,
);
struct TupleFlow;
impl Flow for TupleFlow {
    type Input = TupleGroup;
    type Output = Summary;
    type Checkpoint = ();
    type Resume = MixedResults;

    /// Resumes directly into a heterogeneous result tuple from the persisted array.
    fn advance(
        &self,
        _: TaskId,
        input: Data<TupleGroup>,
        continuation: Continuation<(), MixedResults>,
    ) -> Result<FlowState<Summary>, FlowError> {
        match continuation.into_parts() {
            (None, None) => Ok(FlowState::all(
                (Image(7), Document(u32::from(!input.0.0) * 7)),
                (),
            )?),
            (Some(()), Some(results)) => {
                let (image, document) = results?;
                Ok(FlowState::complete(summarize(image, document)?))
            }
            _ => Err(FlowError::fail("invalid tuple continuation")),
        }
    }
}

struct DynamicFlow;
impl Flow for DynamicFlow {
    type Input = DynamicGroup;
    type Output = Summary;
    type Checkpoint = ();
    type Resume = FlowAllOut;

    /// Uses the dynamic result view without a second extractor or stored representation.
    fn advance(
        &self,
        _: TaskId,
        input: Data<DynamicGroup>,
        continuation: Continuation<(), FlowAllOut>,
    ) -> Result<FlowState<Summary>, FlowError> {
        match continuation.into_parts() {
            (None, None) => Ok(FlowState::all(dynamic_inputs(&input), ())?),
            (Some(()), Some(results)) => {
                let outputs = results?;
                if outputs.is_empty() {
                    return Ok(FlowState::complete(Summary {
                        image: 0,
                        document: None,
                    }));
                }
                Ok(FlowState::complete(summarize(
                    outputs.decode(0)?,
                    outputs.decode(1)?,
                )?))
            }
            _ => Err(FlowError::fail("invalid dynamic continuation")),
        }
    }
}

/// Builds the same mixed inputs through builder and dynamic insertion without wrapper tasks.
fn dynamic_inputs(input: &DynamicGroup) -> FlowAllIn {
    let document = Document(u32::from(!input.failed) * 7);
    match input.mode {
        0 => FlowAllIn::new().with(Image(7)).with(document),
        1 => {
            let mut inputs = FlowAllIn::new();
            inputs.push(Image(7));
            inputs.push(document);
            inputs
        }
        2 => FlowAllIn::new(),
        _ => FlowAllIn::new().with(Image(7)).with(Unknown),
    }
}

/// Child failure remains data while successful image output uses its own concrete schema.
fn summarize(
    image: Result<ImageResult, TaskFailure>,
    document: Result<DocumentResult, TaskFailure>,
) -> Result<Summary, FlowError> {
    Ok(Summary {
        image: image?.width,
        document: document.ok().map(|value| value.content),
    })
}

/// Keeps macro and direct registrations equivalent, including distinct lanes for identical JSON.
fn mixed_bundle(direct: bool) -> bundles::Bundle {
    if direct {
        bundles::bundle([
            bundles::work(mixed_image, TaskDefinition::new("mixed_image").lane(GPU)),
            bundles::work(
                mixed_document,
                TaskDefinition::new("mixed_document").lane(IO),
            ),
            bundles::flow(tuple_group, FlowConf::new("tuple_group")),
            bundles::flow(dynamic_group, FlowConf::new("dynamic_group")),
            bundles::flow(nested_group, FlowConf::new("nested_group").lane(IO)),
            bundles::work_batch(mixed_batch, TaskDefinition::new("mixed_batch").lane(GPU)),
            bundles::flow(extended_group, FlowConf::new("extended_group")),
        ])
    } else {
        bundles::bundle([
            __bundle_part_mixed_image(),
            __bundle_part_mixed_document(),
            __bundle_part_tuple_group(),
            __bundle_part_dynamic_group(),
            __bundle_part_nested_group(),
            __bundle_part_mixed_batch(),
            __bundle_part_extended_group(),
        ])
    }
}

/// Uses independent lane capacity and small output chunks to exercise the normal pipeline.
fn mixed_conf() -> SiteConf {
    SiteConf::default().log_init(false).tasks(
        TaskConf::default()
            .db_concurrency(2)
            .commit_batch_size(1)
            .poll_interval(Duration::from_millis(5))
            .lane(TaskLaneConf::new(GPU, 2))
            .lane(TaskLaneConf::new(IO, 2)),
    )
}

/// Reads a committed terminal result without mutating lifecycle or forcing a claim.
async fn wait_result<T: serde::de::DeserializeOwned>(
    site: &Site,
    id: TaskId,
) -> Result<Result<T, TaskFailure>, TestError> {
    Ok(tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(task) = site.tasks().get(id).await?
                && let Some(result) = task.last_result::<T>()?
                && matches!(task.status(), TaskStatus::Succeeded | TaskStatus::Failed)
            {
                return Ok::<_, TaskRuntimeError>(result);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??)
}

/// Confirms actual registered handler execution, lineage, policies, and completion ordering.
async fn assert_routing(site: &Site, parent: TaskId, failed: bool) -> Result<(), TestError> {
    let tasks = site.tasks().list(TaskFilter::new()).await?.items;
    let children: Vec<&TaskInfo> = tasks
        .iter()
        .filter(|task| task.parent_id() == Some(parent))
        .collect();
    assert_eq!(children.len(), 2);
    for child in &children {
        assert_eq!(child.root_id(), Some(parent));
        assert_eq!(child.kind(), TaskKind::Work);
        assert_eq!(child.attempts(), 1);
    }
    let image = children
        .iter()
        .find(|task| task.name() == "mixed_image")
        .ok_or_else(|| TaskRuntimeError::TaskNotFound("image child".into()))?;
    let document = children
        .iter()
        .find(|task| task.name() == "mixed_document")
        .ok_or_else(|| TaskRuntimeError::TaskNotFound("document child".into()))?;
    assert_eq!(image.lane(), GPU.as_str());
    assert_eq!(document.lane(), IO.as_str());
    assert_eq!(
        image
            .last_result::<ImageResult>()?
            .and_then(Result::ok)
            .map(|value| value.width),
        Some(14)
    );
    assert_eq!(
        document.status(),
        if failed {
            TaskStatus::Failed
        } else {
            TaskStatus::Succeeded
        }
    );
    assert!(document.completed_at() <= image.completed_at());
    if failed {
        assert!(
            matches!(document.last_result::<DocumentResult>()?, Some(Err(error)) if error.task_id() == Some(&document.id()))
        );
    }
    Ok(())
}

/// Tuple and dynamic resumes invoke each original handler on its own lane for both registrations.
#[tokio::test]
async fn mixed_lane_round_trip() -> Result<(), TestError> {
    for direct in [false, true] {
        let site = Site::build(mixed_conf(), mixed_bundle(direct)).await?;
        let runtime = vyuh::testing::TestSite::new(site.clone());
        runtime.start_runtime().await?;
        for failed in [false, true] {
            let mut ids = vec![site.tasks().submit(TupleGroup(failed)).await?.id()];
            for mode in [0, 1] {
                ids.push(
                    site.tasks()
                        .submit(DynamicGroup { mode, failed })
                        .await?
                        .id(),
                );
            }
            for id in ids {
                let expected = Summary {
                    image: 14,
                    document: (!failed).then(|| "doc-7".into()),
                };
                assert_eq!(wait_result(&site, id).await?, Ok(expected));
                assert_routing(&site, id, failed).await?;
            }
        }
        runtime.shutdown_and_wait().await;
    }
    Ok(())
}

/// Empty groups complete normally; an unregistered member inserts no partial children.
#[tokio::test]
async fn empty_and_invalid_groups() -> Result<(), TestError> {
    let site = Site::build(mixed_conf(), mixed_bundle(false)).await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    for mode in [2, 3] {
        let id = site
            .tasks()
            .submit(DynamicGroup {
                mode,
                failed: false,
            })
            .await?
            .id();
        let result = wait_result::<Summary>(&site, id).await?;
        assert_eq!(result.is_ok(), mode == 2);
        assert!(
            site.tasks()
                .list(TaskFilter::new())
                .await?
                .items
                .iter()
                .all(|task| task.parent_id() != Some(id))
        );
    }
    runtime.shutdown_and_wait().await;
    Ok(())
}

/// Flow children and Work batch registrations retain their own kind, lane, lineage and outputs.
#[tokio::test]
async fn mixed_flow_and_batch() -> Result<(), TestError> {
    for direct in [false, true] {
        let site = Site::build(mixed_conf(), mixed_bundle(direct)).await?;
        let runtime = vyuh::testing::TestSite::new(site.clone());
        runtime.start_runtime().await?;
        let parent = site.tasks().submit(ExtendedGroup).await?.id();
        assert_eq!(
            wait_result(&site, parent).await?,
            Ok(("nested-5".to_owned(), 14_u32, 18_u32))
        );
        let tasks = site.tasks().list(TaskFilter::new()).await?.items;
        let children: Vec<_> = tasks
            .iter()
            .filter(|task| task.parent_id() == Some(parent))
            .collect();
        assert_eq!(children.len(), 3);
        for child in children {
            assert_eq!(child.root_id(), Some(parent));
            if child.name() == "nested_group" {
                assert_eq!(child.kind(), TaskKind::Flow);
                assert_eq!(child.lane(), IO.as_str());
            } else {
                assert_eq!(child.name(), "mixed_batch");
                assert_eq!(child.kind(), TaskKind::Work);
                assert_eq!(child.lane(), GPU.as_str());
            }
        }
        runtime.shutdown_and_wait().await;
    }
    Ok(())
}
