use vyuh::testing::TestSite;

#[vyuh::test]
async fn default_fixture(site: &TestSite) -> Result<(), ()> {
    let _ = site;
    Ok(())
}

fn main() {}
