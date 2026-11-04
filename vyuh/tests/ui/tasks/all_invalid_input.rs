use vyuh::prelude::*;

struct NotTask;

fn main() {
    let _ = FlowAllIn::new().with(NotTask);
}
