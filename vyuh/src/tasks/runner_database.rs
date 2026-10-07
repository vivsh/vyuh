//! Runner-owned acknowledgement evidence, poll-paced retries and liveness deadlines.
use super::*;
use crate::tasks::store::StoreFailure;

pub(super) struct InFlight {
    pub(super) request: Arc<DbRequest>,
    pub(super) phase: DbPhase,
}

pub(super) enum DbPhase {
    Running,
    Recovering,
    Retry {
        not_before: tokio::time::Instant,
        prepared: Option<DbResponse>,
    },
}

impl<S: AbstractTaskStore + Send + Sync + 'static> AbstractTaskRunner<S> {
    pub(super) fn stopped(&self) -> bool {
        matches!(
            self.mode,
            RunMode::Retired | RunMode::Dead | RunMode::Faulted | RunMode::Finished
        )
    }

    /// Enforces the terminal deadline before admission or acknowledgement can revive activity.
    pub(super) fn expire(&mut self) -> bool {
        if !self.stopped()
            && self
                .death_at
                .is_some_and(|at| at <= tokio::time::Instant::now())
        {
            tracing::error!(node_id = %self.node_id, "task database contact deadline expired");
            self.mode = RunMode::Dead;
            self.discard_work();
        }
        self.stopped()
    }

    fn death_deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::now()
            + self
                .registry
                .config
                .heartbeat_interval_value()
                .saturating_mul(6)
    }

    /// Derives the next retry wake without maintaining a second recovery queue.
    pub(super) fn retry_deadline(&self) -> Option<tokio::time::Instant> {
        self.in_flight
            .values()
            .filter_map(|entry| match &entry.phase {
                DbPhase::Retry { not_before, .. } => Some(*not_before),
                _ => None,
            })
            .min()
    }

    /// Earliest eligible failure wins; failed retries move to the back by their deadline.
    pub(super) fn retry_due(&self) -> Option<uuid::Uuid> {
        self.in_flight
            .iter()
            .filter_map(|(id, entry)| match &entry.phase {
                DbPhase::Retry { not_before, .. } if *not_before <= tokio::time::Instant::now() => {
                    Some((*not_before, *id))
                }
                _ => None,
            })
            .min()
            .map(|(_, id)| id)
    }

    /// Starts one immutable operation and arms contact liveness while it remains outstanding.
    pub(super) fn start_database(&mut self, request: DbRequest, permit: OwnedSemaphorePermit) {
        let id = uuid::Uuid::now_v7();
        let request = Arc::new(request);
        self.in_flight.insert(
            id,
            InFlight {
                request: request.clone(),
                phase: DbPhase::Running,
            },
        );
        if self.death_at.is_none() {
            self.death_at = Some(self.death_deadline());
        }
        self.database_future(id, request, None, permit);
    }

    /// Transfers retained evidence into a single retry without changing the operation identity.
    pub(super) fn retry_database(&mut self, id: uuid::Uuid, permit: OwnedSemaphorePermit) {
        let Some(entry) = self.in_flight.get_mut(&id) else {
            return;
        };
        let phase = std::mem::replace(&mut entry.phase, DbPhase::Recovering);
        let DbPhase::Retry { prepared, .. } = phase else {
            entry.phase = phase;
            return;
        };
        let request = entry.request.clone();
        self.database_future(id, request, prepared, permit);
    }

    /// The future temporarily owns prepared evidence; every recoverable failure returns it.
    fn database_future(
        &mut self,
        id: uuid::Uuid,
        request: Arc<DbRequest>,
        prepared: Option<DbResponse>,
        permit: OwnedSemaphorePermit,
    ) {
        let store = self.store.clone();
        let node = self.node_id;
        let stale_after = self
            .registry
            .config
            .heartbeat_interval_value()
            .saturating_mul(6);
        self.operations.push(
            async move {
                let started = std::time::Instant::now();
                let call = database_call(&*store, &node, &request, prepared, stale_after);
                let result = std::panic::AssertUnwindSafe(call)
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| {
                        Err(StoreFailure::BeforeCommit(
                            TaskRuntimeError::TaskExecutionError(
                                "task store panicked; acknowledgement evidence unavailable".into(),
                            ),
                        ))
                    });
                DbCompletion {
                    id,
                    result,
                    permit,
                    started,
                }
            }
            .boxed(),
        );
    }

    /// Applies only acknowledged responses; a success cannot revive an expired runner.
    pub(super) fn apply_database(
        &mut self,
        site: &Site,
        hooks: &mpsc::Sender<HookCompletion>,
        completion: DbCompletion,
    ) {
        if self.expire() {
            return;
        }
        let Some(entry) = self.in_flight.get(&completion.id) else {
            return;
        };
        let request = entry.request.clone();
        let result = match completion.result {
            Ok(response) => self
                .apply_response(
                    site,
                    hooks,
                    &request,
                    response,
                    completion.started.elapsed(),
                )
                .map_err(StoreFailure::BeforeCommit),
            Err(error) => Err(error),
        };
        if matches!(&*request, DbRequest::Heartbeat) {
            self.metrics.heartbeat(
                completion.started.elapsed(),
                result.is_err() || self.stopped(),
            );
        }
        match result {
            Err(error) => self.database_failed(completion.id, error),
            Ok(()) if !self.stopped() => {
                self.in_flight.remove(&completion.id);
                self.death_at = (!self.in_flight.is_empty()).then(|| self.death_deadline());
                if self
                    .in_flight
                    .values()
                    .all(|entry| matches!(entry.phase, DbPhase::Running))
                {
                    self.health.succeeded();
                }
            }
            Ok(()) => {}
        }
        drop(completion.permit);
    }

    /// Ordinary database errors retry; protocol and programming errors terminate safely.
    fn database_failed(&mut self, id: uuid::Uuid, failure: StoreFailure<DbResponse>) {
        let (error, prepared) = match failure {
            StoreFailure::BeforeCommit(error) => (error, None),
            StoreFailure::CommitUnknown { error, prepared } => (error, Some(prepared)),
        };
        #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
        let error = match error {
            TaskRuntimeError::DatabaseError(source) => TaskRuntimeError::StoreError(source.into()),
            error => error,
        };
        self.health.store_failed();
        self.metrics.store_failure();
        super::super::diagnostics::log_runtime_error(&error, "task database operation failed");
        if !retryable(&error) {
            self.mode = RunMode::Faulted;
            self.discard_work();
            return;
        }
        if let Some(entry) = self.in_flight.get_mut(&id) {
            entry.phase = DbPhase::Retry {
                not_before: tokio::time::Instant::now() + self.poll_interval,
                prepared,
            };
        }
    }

    /// Correlates one response with its immutable operation, without replaying local effects.
    fn apply_response(
        &mut self,
        site: &Site,
        hooks: &mpsc::Sender<HookCompletion>,
        request: &DbRequest,
        response: DbResponse,
        elapsed: std::time::Duration,
    ) -> Result<(), TaskRuntimeError> {
        match (request, response) {
            (DbRequest::Heartbeat, DbResponse::Heartbeat { present: true }) => {
                self.schedule_heartbeat()
            }
            (DbRequest::Heartbeat, DbResponse::Heartbeat { present: false }) => self.retire(),
            (DbRequest::GiveBack, DbResponse::Returned) => {
                self.in_flight.clear();
                self.death_at = None;
                for lane in &mut self.lanes {
                    lane.tasks.clear();
                }
                self.mode = RunMode::Finished;
            }
            (
                DbRequest::Claim { claims } | DbRequest::Coordinate { lanes: claims },
                DbResponse::Poll(poll),
            ) => {
                validate_poll(claims, &poll)?;
                self.apply_poll(site, hooks, claims, poll);
            }
            (DbRequest::Flush { commits }, DbResponse::Flushed(result)) => {
                self.acknowledge_commits(commits);
                self.metrics.commit(elapsed, false);
                for lane in commits
                    .iter()
                    .filter_map(|commit| commit.lane())
                    .chain(result.wake_lanes)
                {
                    self.wake_lane(lane);
                }
                for id in result.rejected {
                    tracing::warn!(task_id = %id, "obsolete task output resolved without reapplying");
                }
            }
            _ => {
                return Err(TaskRuntimeError::TaskExecutionError(
                    "mismatched task store response".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Only classified operational failures are retried; permanent failures terminate safely.
fn retryable(error: &TaskRuntimeError) -> bool {
    match error {
        TaskRuntimeError::DatabaseError(error) => matches!(
            error,
            crate::db::sqlx::Error::Io(_)
                | crate::db::sqlx::Error::Tls(_)
                | crate::db::sqlx::Error::PoolTimedOut
                | crate::db::sqlx::Error::PoolClosed
                | crate::db::sqlx::Error::WorkerCrashed
        ),
        TaskRuntimeError::StoreError(error) => matches!(
            error.code(),
            "connection_error"
                | "timeout"
                | "serialization_failure"
                | "deadlock"
                | "cancelled"
                | "database_busy"
                | "database_locked"
        ),
        _ => false,
    }
}

/// Uses the same request on every attempt; only uncertain transactions require reads.
async fn database_call<S: AbstractTaskStore>(
    store: &S,
    node: &uuid::Uuid,
    request: &DbRequest,
    prepared: Option<DbResponse>,
    stale_after: std::time::Duration,
) -> Result<DbResponse, StoreFailure<DbResponse>> {
    if let Some(prepared) = prepared {
        return recover_call(store, node, request, prepared).await;
    }
    match request {
        DbRequest::Heartbeat => Ok(DbResponse::Heartbeat {
            present: store.heartbeat(node, stale_after).await?,
        }),
        DbRequest::GiveBack => {
            store.return_claims(Some(*node)).await?;
            Ok(DbResponse::Returned)
        }
        DbRequest::Claim { claims } => store
            .claim_tasks(node, claims)
            .await
            .map(DbResponse::Poll)
            .map_err(|e| e.map(DbResponse::Poll)),
        DbRequest::Coordinate { lanes } => store
            .coordinate_lanes(node, lanes)
            .await
            .map(DbResponse::Poll)
            .map_err(|e| e.map(DbResponse::Poll)),
        DbRequest::Flush { commits } => store
            .commit_tasks(commits)
            .await
            .map(DbResponse::Flushed)
            .map_err(|e| e.map(DbResponse::Flushed)),
    }
}

/// Dispatches bounded recovery without discarding its retained response on failure.
async fn recover_call<S: AbstractTaskStore>(
    store: &S,
    node: &uuid::Uuid,
    request: &DbRequest,
    prepared: DbResponse,
) -> Result<DbResponse, StoreFailure<DbResponse>> {
    match (request, prepared) {
        (
            DbRequest::Claim { claims } | DbRequest::Coordinate { lanes: claims },
            DbResponse::Poll(poll),
        ) => store
            .recover_claims(node, claims, poll)
            .await
            .map(DbResponse::Poll)
            .map_err(|e| e.map(DbResponse::Poll)),
        (DbRequest::Flush { commits }, DbResponse::Flushed(result)) => store
            .recover_outputs(node, commits, result)
            .await
            .map(DbResponse::Flushed)
            .map_err(|e| e.map(DbResponse::Flushed)),
        _ => Err(TaskRuntimeError::TaskExecutionError("invalid recovery evidence".into()).into()),
    }
}
