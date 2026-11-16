use super::*;
use crate::tasks::config::MAX_TASK_DELAY;
use crate::tasks::outcome::normalize_outcome;

/// Work sleep uses the shared checkpoint-byte and duration limits.
#[test]
fn sleep_validates_checkpoint_and_delay() -> Result<(), TaskRuntimeError> {
    let sleep = |state, delay| {
        WorkState::<()>::sleep(state, delay)
            .map(|state| normalize_outcome(state.into_outcome(), 2, 128))
    };
    assert!(matches!(
        sleep(42, MAX_TASK_DELAY)?,
        TaskOutcome::Sleep { .. }
    ));
    assert!(matches!(
        sleep(123, MAX_TASK_DELAY)?,
        TaskOutcome::Fail { .. }
    ));
    assert!(matches!(
        sleep(42, MAX_TASK_DELAY + Duration::from_nanos(1))?,
        TaskOutcome::Fail { .. }
    ));
    Ok(())
}
