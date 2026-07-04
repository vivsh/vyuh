use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "posts")]
struct Post {
    id: i64,
    title: String,
}

fn main() {
    let _ = std::any::type_name::<db::QueryScope>();
    let _ = std::any::type_name::<db::All<Post>>();
    let _ = std::any::type_name::<db::ModelTable<Post>>();
    let _ = std::any::type_name::<db::Predicate>();
    let _ = std::any::type_name::<db::Projectable>();
    let _ = db::__private::table("posts");
}
