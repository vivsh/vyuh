use vyuh::auth::AuthUser;

fn main() {
    let _user = AuthUser::new("user-1").with_role_mask(1);
}
