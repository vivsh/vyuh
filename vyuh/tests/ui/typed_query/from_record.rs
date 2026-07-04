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
    let row = PostRow {
        post: Post {
            id: 1,
            title: "hello".to_string(),
        },
    };
    let _ = db::from(row);
}
