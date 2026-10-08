use super::*;

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Lookup {
    query: String,
}

async fn lookup(input: Lookup, _: pravah::Context) -> Result<String, pravah::tools::ToolError> {
    Ok(format!("found {}", input.query))
}

async fn control(
    input: pravah::AgentLoop<Input>,
    _: pravah::Context,
) -> Result<pravah::AgentDecision, pravah::GraphError> {
    assert_eq!(input.input().0, "question");
    Ok(pravah::AgentDecision::continue_())
}

fn controlled(root: pravah::Flow<Input>) -> pravah::Flow<u32> {
    root.agent(|agent| {
        agent
            .tools(|tools| tools.tool(lookup))
            .control(control)
            .configure(configure)
    })
}

/// Controller and tool callbacks run in committed child Work, using the graph's explicit registry.
#[tokio::test]
async fn controller_and_tools() -> Result<(), TestError> {
    let script = pravah::testing::ScriptedFactory::new()
        .then_tool_calls(vec![pravah::testing::mock_tool_call(
            "lookup-1",
            "lookup",
            serde_json::json!({"query":"evidence"}),
        )])
        .then_output(serde_json::json!(42));
    let site = controlled_site(script.clone()).await?;
    let runtime = vyuh::testing::TestSite::new(site.clone());
    runtime.start_runtime().await?;
    let id = site.tasks().submit(Input("question".into())).await?.id();
    let result = terminal(&site, id).await?;
    runtime.shutdown_and_wait().await;
    assert_eq!(result.last_result::<u32>()?, Some(Ok(42)));
    let calls = script.calls();
    assert_eq!(calls.len(), 2);
    assert!(calls.last().is_some_and(|(_, messages)| {
        messages
            .iter()
            .any(|message| message.content.contains("found evidence"))
    }));
    let children = site
        .tasks()
        .list(vyuh::tasks::TaskFilter::new().name("agent_work"))
        .await?;
    assert!(
        children.total > 4,
        "configure, control, generate and tool boundaries must persist"
    );
    assert!(
        children
            .items
            .iter()
            .all(|task| task.status() == TaskStatus::Succeeded)
    );
    Ok(())
}

/// Uses identical symbolic roots for factory construction and the effect executor's registry.
async fn controlled_site(script: pravah::testing::ScriptedFactory) -> Result<Site, TestError> {
    let compiled = controlled(pravah::Flow::root()).finish::<Input>()?;
    let context = pravah::Context::default().with_providers(pravah::testing::providers(script)?);
    Ok(Site::build(
        SiteConf::default()
            .log_init(false)
            .tasks(TaskConf::default().poll_interval(Duration::from_millis(5))),
        bundles::bundle([
            bundles::flow(
                controlled,
                FlowConf::new("controlled").dispatch::<Dispatcher>(),
            ),
            worker(compiled.prepared().executor(context)),
        ]),
    )
    .await?)
}
