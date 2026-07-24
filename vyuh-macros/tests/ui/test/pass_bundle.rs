use vyuh::{bundles::Bundle, testing::TestSite};

fn app_bundle() -> Bundle {
    Bundle::default()
}

#[vyuh::test(bundle = app_bundle)]
async fn bundled_fixture(site: &TestSite) -> Result<(), ()> {
    let _ = site;
    Ok(())
}

fn main() {}
