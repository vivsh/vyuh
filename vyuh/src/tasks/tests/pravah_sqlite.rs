use super::*;
use crate::{Site, SiteConf, bundles, db, tasks::*, testing::TestSite};

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
        }))
    }
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
        SiteConf::default()
            .log_init(false)
            .tasks(TaskConf::default().poll_interval(Duration::from_millis(5))),
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
    let id = site
        .tasks()
        .submit(1_u32)
        .await
        .map_err(|e| e.to_string())?
        .id();
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
        db::sqlx::query(&sql)
            .execute(&pool)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(pool)
}
