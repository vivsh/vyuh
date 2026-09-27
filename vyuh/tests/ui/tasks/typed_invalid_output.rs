use vyuh::prelude::*;
fn wrong_work() -> TaskState<u32> { TaskState::complete("wrong") }
fn wrong_flow() -> FlowState<u32> { FlowState::complete("wrong") }
fn main() {}
