use vyuh::db;

#[derive(Debug, Clone, db::Model)]
struct Post {
    id: i64,
    title: String,
}

#[derive(Debug, Clone, db::Filterable)]
#[filter(model = Post)]
struct PostFilter {
    #[filter(ilike, column(title))]
    q: Option<String>,
}

fn main() {}
