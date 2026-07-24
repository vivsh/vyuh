use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "posts")]
struct Post {
    id: i64,
    title: String,
}

fn main() {
    fn order(builder: db::FilterBuilder<Post>) {
        let posts = Post::table();
        let _ = builder.order_by(posts.id.asc());
    }

    let _ = order;
}
