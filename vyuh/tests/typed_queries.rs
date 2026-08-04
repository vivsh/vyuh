#![cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]

use vyuh::prelude::*;

#[derive(Debug, Clone, db::Model)]
#[table(name = "facade_posts")]
struct FacadePost {
    id: i64,
    published: bool,
}

/// Verifies Vyuh exposes Mool's active-backend query API through its database facade.
#[test]
fn facade_plans_a_typed_query() {
    let posts = FacadePost::table();
    let plan = db::from(&posts)
        .filter(posts.published.eq(db::val(true)))
        .all::<FacadePost>()
        .plan();

    assert!(matches!(
        plan,
        Ok(plan) if plan.sql.contains("SELECT facade_posts.id, facade_posts.published FROM facade_posts")
    ));
}
