use vyuh::testing::TestSite;

#[vyuh::test]
fn not_async(site: &TestSite) -> Result<(), ()> {
    let _ = site;
    Ok(())
}

fn main() {}
