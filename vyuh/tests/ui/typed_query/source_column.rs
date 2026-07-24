use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "posts")]
struct Post {
    id: i64,
    title: String,
}

#[derive(Debug, Clone, db::Record)]
#[table(name = "posts")]
struct PostId {
    id: i64,
}

fn main() {
    let posts = Post::table();
    let ids = match db::from(&posts)
        .all::<PostId>()
        .set(db::out::<PostId>().id, &posts.id)
        .subquery()
    {
        Ok(ids) => ids,
        Err(_) => return,
    };

    let _ = ids.column::<i64>("id");
}
