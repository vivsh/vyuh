use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "posts")]
struct Post {
    id: i64,
    title: String,
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "posts")]
struct PostPatch {
    title: String,
}

fn main() {
    let posts = Post::table();
    let patch = PostPatch {
        title: "Draft".to_string(),
    };
    let _ = db::from(&posts).update(&patch).cte();
}
