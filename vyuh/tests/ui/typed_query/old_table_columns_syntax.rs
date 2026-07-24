use vyuh::db;

#[derive(Debug, Clone, db::Model)]
#[table(primary_key(name = "memberships_identity", columns("tenant_id", "user_id")))]
struct Membership {
    tenant_id: i64,
    user_id: i64,
}

fn main() {}
