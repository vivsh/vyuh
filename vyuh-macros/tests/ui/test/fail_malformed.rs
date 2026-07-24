use vyuh::testing::TestSite;

#[vyuh::test(conf)]
async fn malformed_option(site: &TestSite) -> Result<(), ()> {
    let _ = site;
    Ok(())
}

fn main() {}
