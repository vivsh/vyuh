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
        title: "updated".to_string(),
    };
    let _ = db::from(&posts)
        .all::<Post>()
        .set(db::out::<Post>().id, &posts.id)
        .update(&patch);
}
