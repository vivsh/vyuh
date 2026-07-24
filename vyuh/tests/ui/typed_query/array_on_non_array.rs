use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "posts")]
struct Post {
    id: i64,
    title: String,
}

fn main() {
    let posts = Post::table();
    let _ = db::funcs::array::length(&posts.title);
}
