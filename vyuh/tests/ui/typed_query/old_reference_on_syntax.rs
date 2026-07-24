use vyuh::db;

#[derive(Debug, Clone, db::Model)]
struct User {
    id: i64,
}

#[derive(Debug, Clone, db::Model)]
struct Post {
    id: i64,
    author_id: i64,
}

#[derive(Debug, Clone, db::Record)]
struct PostWithAuthor {
    #[column(flatten)]
    post: Post,

    #[column(reference(on(author_id, id)))]
    author: User,
}

fn main() {}
