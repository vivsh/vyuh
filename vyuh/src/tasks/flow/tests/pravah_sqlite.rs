use super::*;
use crate::{Site, SiteConf, bundles, db, tasks::*, testing::TestSite};

const AGENT_LANE: TaskLane = TaskLane::new("agent-override");

#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct AgentJob {
    #[schemars(with = "serde_json::Value")]
    request: pravah::AgentRequest,
}

struct Dispatcher;
impl PravahDispatcher for Dispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> {
        Ok(Self)
    }
    fn agent(&self, request: &pravah::AgentRequest) -> Result<WorkRequest, FlowError> {
        Ok(WorkRequest::new(AgentJob {
            request: request.clone(),
        })
        .options(TaskOptions::new().lane(AGENT_LANE))?)
    }
}

struct WaitDispatcher;

impl PravahDispatcher for WaitDispatcher {
    fn build(_: &PartialSite) -> Result<Self, FlowError> {
        Ok(Self)
    }

    fn agent(&self, _: &pravah::AgentRequest) -> Result<WorkRequest, FlowError> {
        Err(FlowError::MissingDispatcher)
    }

    fn suspend(&self, wait: &pravah::Suspension) -> Result<Option<WorkRequest>, FlowError> {
        Ok((wait.payload().as_u64() == Some(7)).then(|| WorkRequest::new("answer".to_owned())))
    }

    fn wait_topic(&self, wait: &pravah::Suspension) -> Result<Option<String>, FlowError> {
        match wait.payload().as_u64() {
            Some(42) => Ok(Some("approval:42".into())),
            Some(0) => Ok(None),
            Some(1) => Ok(Some("  ".into())),
            Some(7) => Err(FlowError::fail("Work dispatch must take precedence")),
            _ => Err(FlowError::fail("Unexpected suspension payload")),
        }
    }
}

fn waiting_graph(root: pravah::Flow<u32>) -> pravah::Flow<u32> {
    root.suspend::<u32>()
}

/// The real scheduler delivers a typed publication, direct resume, or child result
/// according to dispatcher routing, without changing the graph or checkpoint.
#[tokio::test]
async fn sqlite_suspension_routes() -> Result<(), String> {
    let pool = fresh_pool().await?;
    let graph = bundles::flow(
        waiting_graph,
        FlowConf::new("await_approval").dispatch::<WaitDispatcher>(),
    );
    let work = bundles::work(
        |Data(input): Data<String>| async move {
            if input.as_str() == "answer" {
                17_u32
            } else {
                0
            }
        },
        TaskDefinition::new("answer_work"),
    );
    let site = Site::test(
        SiteConf::default()
            .log_init(false)
            .tasks(TaskConf::default().poll_interval(Duration::from_millis(5))),
        bundles::bundle([graph, work]),
        pool.clone(),
    )
    .await
    .map_err(error)?;
    let runtime = TestSite::new(site.clone());
    runtime.start_runtime().await.map_err(error)?;
    let result = check_routes(&site).await;
    runtime.shutdown_and_wait().await;
    pool.close().await;
    result
}

/// Verifies fallback, Work precedence, and invalid-topic failure against persisted rows.
async fn check_routes(site: &Site) -> Result<(), String> {
    let tasks = site.tasks();
    check_fanout(&tasks).await?;
    let direct = submit(&tasks, 0).await?;
    let work = submit(&tasks, 7).await?;
    let invalid = submit(&tasks, 1).await?;
    await_status(&tasks, direct, TaskStatus::Suspended).await?;
    assert!(tasks.resume(direct, 11_u32).await.map_err(error)?);
    check_result(&tasks, direct, 11).await?;
    check_result(&tasks, work, 17).await?;
    let task = await_status(&tasks, invalid, TaskStatus::Failed).await?;
    let Some(Err(failure)) = task.last_result::<u32>().map_err(error)? else {
        return Err("missing invalid topic failure".into());
    };
    assert_eq!(failure.message(), "Invalid Pravah suspension wait topic");
    let children = tasks
        .list(TaskFilter::new().name("answer_work"))
        .await
        .map_err(error)?;
    assert_eq!(children.total, 1);
    assert_eq!(
        children.items.first().and_then(TaskInfo::parent_id),
        Some(work)
    );
    Ok(())
}

/// Topic publication only consumes committed, matching waits and delivers a typed result.
async fn check_fanout(tasks: &Tasks) -> Result<(), String> {
    let topic = "approval:42";
    assert_eq!(tasks.publish(topic, 5_u32).await.map_err(error)?.resumed, 0);
    let a = submit(tasks, 42).await?;
    let b = submit(tasks, 42).await?;
    for id in [a, b] {
        await_status(tasks, id, TaskStatus::Suspended).await?;
    }
    assert!(!tasks.resume(a, 5_u32).await.map_err(error)?);
    assert!(
        !tasks
            .resume_failed(b, TaskFailure::new(None, "not a direct wait"))
            .await
            .map_err(error)?
    );
    assert_eq!(
        tasks
            .publish("unrelated", 5_u32)
            .await
            .map_err(error)?
            .resumed,
        0
    );
    assert_eq!(
        tasks.publish(topic, 23_u32).await.map_err(error)?,
        PublishResult {
            resumed: 2,
            more: false
        }
    );
    assert_eq!(tasks.publish(topic, 5_u32).await.map_err(error)?.resumed, 0);
    check_result(tasks, a, 23).await?;
    check_result(tasks, b, 23).await
}

async fn submit(tasks: &Tasks, input: u32) -> Result<TaskId, String> {
    tasks
        .submit(input)
        .await
        .map_err(error)?
        .queued_id()
        .map_err(error)
}

async fn check_result(tasks: &Tasks, id: TaskId, expected: u32) -> Result<(), String> {
    let task = await_status(tasks, id, TaskStatus::Succeeded).await?;
    assert_eq!(
        task.last_result::<u32>().map_err(error)?,
        Some(Ok(expected))
    );
    Ok(())
}

/// Polls only durable status and reports an unexpected terminal result immediately.
async fn await_status(tasks: &Tasks, id: TaskId, expected: TaskStatus) -> Result<TaskInfo, String> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let task = tasks.get(id).await.map_err(error)?.ok_or("missing task")?;
            if task.status() == expected {
                return Ok(task);
            }
            if matches!(task.status(), TaskStatus::Succeeded | TaskStatus::Failed) {
                return Err(format!("expected {expected:?}, found {:?}", task.status()));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(error)?
}

fn error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

/// Exercises the adapter through actual SQL child commits and later parent claims.
#[tokio::test]
async fn sqlite_agent_round_trip() -> Result<(), String> {
    let pool = fresh_pool().await?;
    let script = pravah::testing::ScriptedFactory::new().then_output(serde_json::json!(42));
    let compiled = agent_graph(pravah::Flow::root())
        .finish::<u32>()
        .map_err(|e| e.to_string())?;
    let context = pravah::Context::default()
        .with_providers(pravah::testing::providers(script.clone()).map_err(|e| e.to_string())?);
    let executor = Arc::new(compiled.prepared().executor(context));
    let work = bundles::work(
        move |input: Data<AgentJob>| {
            let executor = executor.clone();
            async move { executor.execute(&input.request).await }
        },
        TaskDefinition::new("agent_work"),
    );
    let site = Site::test(
        SiteConf::default().log_init(false).tasks(
            TaskConf::default()
                .poll_interval(Duration::from_millis(5))
                .lane(TaskLaneConf::new(AGENT_LANE, 2)),
        ),
        bundles::bundle([
            bundles::flow(agent_graph, FlowConf::new("graph").dispatch::<Dispatcher>()),
            work,
        ]),
        pool.clone(),
    )
    .await
    .map_err(|e| e.to_string())?;
    let runtime = TestSite::new(site.clone());
    runtime.start_runtime().await.map_err(|e| e.to_string())?;
    let result = run_graph(&site).await;
    runtime.shutdown_and_wait().await;
    result?;
    assert_eq!(script.calls().len(), 1);
    pool.close().await;
    Ok(())
}

/// Observes persisted parent and child results after normal scheduler processing.
async fn run_graph(site: &Site) -> Result<(), String> {
    let id = submit(&site.tasks(), 1).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let task = site
                .tasks()
                .get(id)
                .await
                .map_err(|e| e.to_string())?
                .ok_or("missing task")?;
            if matches!(task.status(), TaskStatus::Succeeded | TaskStatus::Failed) {
                assert_eq!(
                    task.last_result::<u16>().map_err(|e| e.to_string())?,
                    Some(Ok(42))
                );
                let children = site
                    .tasks()
                    .list(TaskFilter::new().name("agent_work"))
                    .await
                    .map_err(|e| e.to_string())?;
                assert_eq!(children.total, 2);
                for child in children.items {
                    assert_eq!(child.parent_id(), Some(id));
                    assert_eq!(child.lane(), AGENT_LANE.as_str());
                    assert_eq!(child.status(), TaskStatus::Succeeded);
                    assert!(matches!(
                        child
                            .last_result::<pravah::AgentResponse>()
                            .map_err(|e| e.to_string())?,
                        Some(Ok(_))
                    ));
                }
                return Ok::<_, String>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|e| e.to_string())?
}

/// Creates only an in-memory SQLite database from the task-owned desired schema.
async fn fresh_pool() -> Result<db::Pool, String> {
    let pool = db::sqlx::pool::PoolOptions::<db::Database>::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .map_err(|e| e.to_string())?;
    let planner = db::gaman::OfflinePlanner::new(db::Dialect::Sqlite);
    let schema = crate::tasks::store::database::schema::task_schema().map_err(|e| e.to_string())?;
    let migration = planner
        .make_migration(schema, &[])
        .map_err(|e| e.to_string())?
        .ok_or("missing task schema")?;
    for sql in planner
        .sql_migrate(&[migration])
        .map_err(|e| e.to_string())?
    {
        db::sqlx::query(db::sqlx::AssertSqlSafe(sql))
            .execute(&pool)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(pool)
}
