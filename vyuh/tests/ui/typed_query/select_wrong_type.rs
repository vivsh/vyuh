use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "posts")]
struct Post {
    id: i64,
    title: String,
}

fn main() {
    let posts = Post::table();
    let _ = db::from(&posts)
        .all::<Post>()
        .set(db::out::<Post>().id, db::val("wrong".to_string()));
}
