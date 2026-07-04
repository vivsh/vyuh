use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "posts")]
struct Post {
    id: i64,
    title: String,
}

#[derive(Debug, Clone, db::Record)]
struct PostRow {
    #[column(flatten)]
    post: Post,
}

fn main() {
    let _ = PostRow::cols();
    let _ = PostRow::pick;
}
