use vyuh::tasks::{TaskState, TaskError, TaskContext, BatchTaskContext, TaskCallable,
    IntoTaskOutcomePart, IntoTaskBatchOutcomePart};

#[vyuh::bundles::task]
async fn old_work() {}

#[vyuh::bundles::task_batch]
async fn old_batch() {}

fn main() {
    let _ = vyuh::bundles::task;
    let _ = vyuh::bundles::task_batch;
}
