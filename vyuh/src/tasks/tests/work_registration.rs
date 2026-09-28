use super::*;
use crate::tasks::{
    Batch, TaskIdempotency, TaskKind, TaskLane, TaskLaneConf,
    store::{MemoryTaskStore, policy_fingerprint},
};
use crate::{Data, bundles};

const LANE: TaskLane = TaskLane::new("legacy-lane");
const KEY: TaskIdempotency<Job> = TaskIdempotency::new("v1", |job: &Job| job.0.to_string());

#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct Job(u32);

#[bundles::work(lane = LANE, idempotency = KEY)]
async fn legacy_name(_: Data<Job>) {}

#[bundles::work(name = "stored-name", lane = LANE, idempotency = KEY)]
async fn explicit_name(_: Data<Job>) {}

#[bundles::work_batch(name = "stored-batch", lane = LANE, idempotency = KEY)]
async fn explicit_batch(_: Data<Batch<Job>>) {}

struct Methods;

impl Methods {
    async fn renamed_function(_: Data<Job>) {}

    async fn batch(_: Data<Batch<Job>>) {}
}

/// Free-function registration keeps the function-derived durable name and policy.
#[test]
fn default_name_is_stable() -> Result<(), TaskRuntimeError> {
    compare(
        bundles::bundle([__bundle_part_legacy_name()]),
        bundles::bundle([bundles::work(legacy_name, definition("legacy_name"))]),
        "legacy_name",
        false,
    )
}

/// Explicit macro names and direct method registration preserve the declared task name.
#[test]
fn method_name_is_stable() -> Result<(), TaskRuntimeError> {
    compare(
        bundles::bundle([__bundle_part_explicit_name()]),
        bundles::bundle([bundles::work(
            Methods::renamed_function,
            definition("stored-name"),
        )]),
        "stored-name",
        false,
    )
}

/// Batch Work registration retains the same input identity, kind, lane and fingerprint.
#[test]
fn batch_policy_is_stable() -> Result<(), TaskRuntimeError> {
    compare(
        bundles::bundle([__bundle_part_explicit_batch()]),
        bundles::bundle([bundles::work_batch(
            Methods::batch,
            definition("stored-batch"),
        )]),
        "stored-batch",
        true,
    )
}

fn definition(name: &str) -> TaskDefinition<Job> {
    TaskDefinition::new(name).lane(LANE).idempotency(KEY)
}

/// Compares immutable registration and store configuration without starting a runtime.
fn compare(
    macro_bundle: bundles::Bundle,
    direct_bundle: bundles::Bundle,
    name: &str,
    batch: bool,
) -> Result<(), TaskRuntimeError> {
    let conf = TaskConf::default().lane(TaskLaneConf::new(LANE, 2));
    let left = Arc::new(macro_bundle.tasks.finalize(conf.clone())?);
    let right = Arc::new(direct_bundle.tasks.finalize(conf)?);
    for registry in [&left, &right] {
        let task = registry
            .tasks
            .get(name)
            .ok_or_else(|| TaskRuntimeError::InvalidConfig("missing stable registration".into()))?;
        assert_eq!(task.name(), name);
        assert_eq!(task.operation().name, name);
        assert_eq!(task.operation().kind, crate::OperationKind::Task);
        assert_eq!(task.kind(), TaskKind::Work);
        assert_eq!(task.effective_lane(), LANE);
        assert_eq!(task.is_batch(), batch);
        assert_eq!(task.idempotency_key(&Job(7))?.as_deref(), Some("7"));
        assert_eq!(
            registry
                .typed_map
                .get(&TypeId::of::<Job>())
                .map(String::as_str),
            Some(name)
        );
    }
    let store = Arc::new(MemoryTaskStore::new(32));
    let left = left.dispatcher(store.clone(), Vec::new()).store_conf()?;
    let right = right.dispatcher(store, Vec::new()).store_conf()?;
    assert_eq!(left.handlers, vec![(name.to_string(), TaskKind::Work)]);
    assert_eq!(left.handlers, right.handlers);
    assert_eq!(left.idempotency, right.idempotency);
    assert_eq!(policy_fingerprint(&left), policy_fingerprint(&right));
    Ok(())
}
