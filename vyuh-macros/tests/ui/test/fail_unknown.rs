use vyuh::testing::TestSite;

#[vyuh::test(database = test_database)]
async fn invalid_option(site: &TestSite) -> Result<(), ()> {
    let _ = site;
    Ok(())
}

fn main() {}
