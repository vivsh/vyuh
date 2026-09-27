use vyuh::prelude::*;
fn main() {
    let _ = TaskState::<()>::retry("retry");
    let _ = TaskState::<()>::fail("fail");
    let _ = FlowState::<()>::fail("fail");
    let _ = FlowError::retry("retry");
}
