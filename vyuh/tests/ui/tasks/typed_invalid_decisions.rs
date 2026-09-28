use vyuh::prelude::*;
fn main() {
    let _ = WorkState::<()>::retry("retry");
    let _ = WorkState::<()>::fail("fail");
    let _ = FlowState::<()>::fail("fail");
    let _ = FlowError::retry("retry");
}
