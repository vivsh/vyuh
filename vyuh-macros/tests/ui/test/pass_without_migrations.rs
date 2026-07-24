use vyuh::testing::TestSite;

#[vyuh::test(migrations = false)]
async fn empty_schema(site: &TestSite) -> Result<(), ()> {
    let _ = site;
    Ok(())
}

fn main() {}
