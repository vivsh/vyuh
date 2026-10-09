//! Process-local pipeline coordination; durable task state belongs to the store.
mod database;
mod engine;
mod execution;
mod lanes;
pub(crate) use engine::TaskRunner;
