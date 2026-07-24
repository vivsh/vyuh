use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "posts")]
struct Post {
    id: i64,
    title: String,
}

fn main() {
    let posts = Post::table();
    let post = Post {
        id: 1,
        title: "draft".to_string(),
    };
    let _ = db::from(&posts)
        .insert(&post)
        .set(&posts.id, db::val("wrong".to_string()));
}
