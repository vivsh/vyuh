use vyuh::tasks::TaskState;

fn main() {
    let _ = TaskState::<()>::all(Vec::<()>::new(), ());
}
