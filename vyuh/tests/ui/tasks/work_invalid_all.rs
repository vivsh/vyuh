use vyuh::tasks::WorkState;

fn main() {
    let _ = WorkState::<()>::all(Vec::<()>::new(), ());
}
