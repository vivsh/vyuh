use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "posts")]
struct Post {
    id: i64,
    published: bool,
}

#[derive(Debug, Clone, db::Filterable)]
#[filter(model = Post)]
struct PostFilter {
    #[filter(op = "ilike", column = "published")]
    q: Option<String>,
}

fn main() {
    let posts = Post::table();
    let filter = PostFilter {
        q: Some("yes".to_string()),
    };
    let _ = db::from(&posts).filter_with(&filter).all::<Post>();
}
