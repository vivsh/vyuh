use std::{collections::VecDeque, sync::Arc};

use tokio::sync::OwnedSemaphorePermit;

use crate::{
    Site,
    tasks::{
        TaskDispatcher, TaskError, TaskListFilter, TaskListPage, TaskOutcome, TaskRecord,
        TaskRegistry,
    },
};

pub trait AbstractTaskStore {
    fn claim_tasks<'a>(
        &'a self,
        runner_id: &'a str,
    ) -> impl Future<Output = Result<Vec<TaskRecord>, TaskError>> + Send + 'a;

    fn commit_outcome<'a>(
        &'a self,
        task_id: uuid::Uuid,
        runner_id: &'a str,
        outcome: TaskOutcome,
    ) -> impl Future<Output = Result<(), TaskError>> + Send + 'a;

    fn store_task(
        &self,
        record: TaskRecord,
    ) -> impl Future<Output = Result<(), TaskError>> + Send + '_;

    fn resume<'a>(
        &'a self,
        id: uuid::Uuid,
        input: String,
    ) -> impl Future<Output = Result<u64, TaskError>> + Send + 'a;

    fn list_tasks(
        &self,
        filter: TaskListFilter,
    ) -> impl Future<Output = Result<TaskListPage, TaskError>> + Send + '_;

    fn get_task(
        &self,
        id: uuid::Uuid,
    ) -> impl Future<Output = Result<Option<TaskRecord>, TaskError>> + Send + '_;

    #[deprecated(
        note = "Task schema provisioning is migration-command owned; apply migrations before starting workers"
    )]
    fn run_migrations(&self) -> impl Future<Output = Result<(), TaskError>> + Send + '_;
}

impl<T: AbstractTaskStore + ?Sized> AbstractTaskStore for Arc<T> {
    fn claim_tasks<'a>(
        &'a self,
        runner_id: &'a str,
    ) -> impl Future<Output = Result<Vec<TaskRecord>, TaskError>> + Send + 'a {
        (**self).claim_tasks(runner_id)
    }

    fn commit_outcome<'a>(
        &'a self,
        task_id: uuid::Uuid,
        runner_id: &'a str,
        outcome: TaskOutcome,
    ) -> impl Future<Output = Result<(), TaskError>> + Send + 'a {
        (**self).commit_outcome(task_id, runner_id, outcome)
    }

    fn store_task(
        &self,
        record: TaskRecord,
    ) -> impl Future<Output = Result<(), TaskError>> + Send + '_ {
        (**self).store_task(record)
    }

    fn resume<'a>(
        &'a self,
        id: uuid::Uuid,
        input: String,
    ) -> impl Future<Output = Result<u64, TaskError>> + Send + 'a {
        (**self).resume(id, input)
    }

    fn list_tasks(
        &self,
        filter: TaskListFilter,
    ) -> impl Future<Output = Result<TaskListPage, TaskError>> + Send + '_ {
        (**self).list_tasks(filter)
    }

    fn get_task(
        &self,
        id: uuid::Uuid,
    ) -> impl Future<Output = Result<Option<TaskRecord>, TaskError>> + Send + '_ {
        (**self).get_task(id)
    }

    #[allow(deprecated)]
    fn run_migrations(&self) -> impl Future<Output = Result<(), TaskError>> + Send + '_ {
        (**self).run_migrations()
    }
}

pub struct AbstractTaskRunner<S: AbstractTaskStore + Send + Sync + 'static> {
    task_queue: VecDeque<Arc<TaskRecord>>,
    capacity: usize,
    inflight: usize,
    concurrency: usize,
    poll_interval: tokio::time::Duration,
    runner_id: String,
    notifier: Arc<tokio::sync::Notify>,
    registry: Arc<TaskRegistry>,
    store: Arc<S>,
}

impl<T: AbstractTaskStore + Send + Sync + 'static> std::fmt::Debug for AbstractTaskRunner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRunner")
            .field("capacity", &self.capacity)
            .field("inflight", &self.inflight)
            .field("task_queue_len", &self.task_queue.len())
            .finish()
    }
}

impl<S: AbstractTaskStore + Send + Sync + 'static> AbstractTaskRunner<S> {
    pub fn new(dispatcher: TaskDispatcher<S>) -> Self {
        let poll_interval_ms = dispatcher.registry.config.poll_interval_ms;
        let capacity = dispatcher.registry.config.capacity.max(1);
        let concurrency = dispatcher.registry.config.concurrency.max(1);
        Self {
            task_queue: VecDeque::new(),
            inflight: 0,
            capacity,
            concurrency,
            poll_interval: tokio::time::Duration::from_millis(poll_interval_ms as u64),
            runner_id: uuid::Uuid::now_v7().to_string(),
            notifier: dispatcher.notifier.clone(),
            store: dispatcher.store.clone(),
            registry: dispatcher.registry.clone(),
        }
    }

    fn can_load_tasks(&self) -> bool {
        self.inflight < self.capacity && self.task_queue.len() < (self.capacity / 2).max(1)
    }

    fn insert_task(&mut self, task: Arc<TaskRecord>) {
        self.inflight += 1;
        self.task_queue.push_back(task);
    }

    async fn load_tasks(&mut self) -> Option<usize> {
        if !self.can_load_tasks() {
            return None;
        }

        match self.store.claim_tasks(&self.runner_id).await {
            Ok(tasks) => {
                let count = tasks.len();
                for task in tasks {
                    self.insert_task(Arc::new(task));
                }
                Some(count)
            }
            Err(e) => {
                tracing::error!("Failed to load tasks: {}", e);
                Some(0)
            }
        }
    }

    pub fn run_concurrently(
        &self,
        site: Site,
        permit: OwnedSemaphorePermit,
        record: Arc<TaskRecord>,
    ) -> tokio::task::JoinHandle<()> {
        let engine = self.registry.clone();
        let store = self.store.clone();
        let runner_id = self.runner_id.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let task_id = record.id;
            let outcome = engine.execute(site, record).await;
            if let Err(err) = store.commit_outcome(task_id, &runner_id, outcome).await {
                tracing::error!("Failed to commit task {} outcome: {}", task_id, err);
            }
        })
    }

    pub async fn run(mut self, site: Site) {
        let shutdown = site.shutdown_notifier();
        let mut backoff_delay = self.poll_interval;
        let max_backoff = self.poll_interval * 32;
        let sem = Arc::new(tokio::sync::Semaphore::new(self.concurrency));

        loop {
            let loaded = self.load_tasks().await;
            if let Some(count) = loaded {
                if count == 0 {
                    backoff_delay = (backoff_delay * 2).min(max_backoff);
                    tracing::debug!("No tasks loaded, backing off to {:?}", backoff_delay);
                } else {
                    backoff_delay = self.poll_interval;
                    tracing::info!("Loaded {} tasks", count);
                }
            }

            tokio::select! {
                _ = shutdown.notified() => {
                    tracing::info!("TaskRunner shutting down");
                    break;
                },
                permit_result = sem.clone().acquire_owned(), if !self.task_queue.is_empty() => {
                    match permit_result {
                        Ok(permit) => {
                            if let Some(record) = self.task_queue.pop_front() {
                                self.inflight = self.inflight.saturating_sub(1);
                                self.run_concurrently(site.clone(), permit, record);
                            }
                        },
                        Err(e) => {
                            tracing::error!("Failed to acquire semaphore permit: {}", e);
                            break;
                        }
                    }
                },
                _ = self.notifier.notified() => {
                    backoff_delay = self.poll_interval;
                },
                _ = tokio::time::sleep(backoff_delay) => {},
            }
        }
    }
}
