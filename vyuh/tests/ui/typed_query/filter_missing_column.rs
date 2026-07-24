use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "posts")]
struct Post {
    id: i64,
    title: String,
}

#[derive(Debug, Clone, db::Filterable)]
#[filter(model = Post)]
struct PostFilter {
    #[filter(op = "eq", column = "missing")]
    title: Option<String>,
}

fn main() {
    let posts = Post::table();
    let filter = PostFilter {
        title: Some("hello".to_string()),
    };
    let _ = db::from(&posts).filter_with(&filter).all::<Post>();
}
