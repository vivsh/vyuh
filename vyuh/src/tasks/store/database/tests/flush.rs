use super::super::claim::tests::row;
use super::*;

/// One prepared command map rejects both duplicate outputs and mixed cancellation duplicates.
#[test]
fn preparation_rejects_duplicates() -> Result<(), TaskRuntimeError> {
    let output = command(1, TaskOutcome::Complete);
    let cancel = TaskCommit::Cancel {
        task_id: output.task_id(),
    };
    assert!(prepare_commits(&[output.clone(), output.clone()], 32, 256).is_err());
    assert!(prepare_commits(&[output, cancel], 32, 256).is_err());
    assert!(prepare_commits(&[], 32, 256)?.is_empty());
    Ok(())
}

/// Entry preparation keeps the weighted All budget even when called by a recovery operation.
#[test]
fn preparation_preserves_budget() -> Result<(), TaskRuntimeError> {
    let commands = [
        command(
            1,
            TaskOutcome::All {
                state: "null".into(),
                children: Vec::new(),
            },
        ),
        command(2, TaskOutcome::Complete),
    ];
    assert!(prepare_commits(&commands, 1, 1).is_err());
    assert_eq!(prepare_commits(&commands, 2, 1)?.len(), 2);
    Ok(())
}

/// The shared command map preserves retry data, cancellation precedence, and stale rejection.
#[test]
fn prepared_acceptance_preserves_outcomes() -> Result<(), TaskRuntimeError> {
    let now = Utc::now();
    let commits = [
        command(1, TaskOutcome::retry("again")),
        command(2, TaskOutcome::Complete),
        command(3, TaskOutcome::Complete),
        TaskCommit::Cancel {
            task_id: TaskId::new(uuid::Uuid::from_u128(4)),
        },
    ];
    let mut commands = prepare_commits(&commits, 32, 256)?;
    let mut rows: Vec<_> = (1..=4)
        .map(|id| {
            let mut row = row(id, now, None);
            row.status = 1;
            row.node_id = Some(uuid::Uuid::from_u128(100));
            row.attempts = if id == 3 { 2 } else { 1 };
            row.step_attempts = 1;
            row.cancelled = id == 2;
            row
        })
        .collect();
    let allowed = commands.keys().copied().collect();
    accept_rows(&mut rows, &commands, &allowed)?;
    assert_eq!(rows.len(), 3);
    let conf = crate::tasks::TaskRegistry::new()
        .with_config(crate::tasks::TaskConf::default())?
        .store_conf()?;
    assert!(
        apply_owned_outcomes(&mut rows, &mut commands, &HashSet::new(), &conf, now)?.is_empty()
    );
    assert_eq!(
        commands.keys().copied().collect::<Vec<_>>(),
        vec![commits.get(2).ok_or_else(missing)?.task_id()]
    );
    assert_preserved(&rows)
}

/// Retry keeps resume/checkpoint bytes; both cancellation routes clear only consumed input.
fn assert_preserved(rows: &[TaskRow]) -> Result<(), TaskRuntimeError> {
    let retry = rows.first().ok_or_else(missing)?;
    assert_eq!(retry.status, 0);
    assert_eq!(retry.state.as_deref(), Some("{}"));
    assert_eq!(retry.resume_input.as_deref(), Some("{\"Ok\":null}"));
    for cancelled in rows.iter().skip(1) {
        assert_eq!(cancelled.status, 4);
        assert!(cancelled.cancelled);
        assert_eq!(cancelled.state.as_deref(), Some("{}"));
        assert!(cancelled.resume_input.is_none());
        assert!(
            cancelled
                .last_result
                .as_deref()
                .is_some_and(|r| r.contains("Task cancelled"))
        );
    }
    Ok(())
}

fn missing() -> TaskRuntimeError {
    TaskRuntimeError::TaskExecutionError("missing fixture".into())
}

fn command(id: u128, outcome: TaskOutcome) -> TaskCommit {
    TaskCommit::Outcome {
        node_id: uuid::Uuid::from_u128(100),
        attempts: 1,
        task_id: TaskId::new(uuid::Uuid::from_u128(id)),
        lane: crate::tasks::DEFAULT_TASK_LANE,
        outcome,
        owner_token: None,
    }
}
