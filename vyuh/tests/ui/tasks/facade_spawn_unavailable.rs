use vyuh::tasks::{TaskOptions, Tasks};

fn forbidden(tasks: &Tasks) {
    let _ = tasks.spawn((), ());
    let _ = tasks.spawn_with((), (), TaskOptions::new());
}

fn main() {}
