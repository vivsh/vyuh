use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "posts")]
struct Post {
    id: i64,
    title: String,
}

fn main() {
    let posts = Post::table();
    let out = db::out::<Post>();
    let _ = db::from(&posts).filter(out.id.eq(db::val(1_i64)));
}
