use vyuh::testing::TestSite;

#[vyuh::test(migrations = enabled)]
async fn invalid_migrations(site: &TestSite) -> Result<(), ()> {
    let _ = site;
    Ok(())
}

fn main() {}
