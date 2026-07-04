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
    let posts = Post::table();
    let _ = posts.cols_for::<PostRow>();
}
