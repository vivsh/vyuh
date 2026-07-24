use vyuh::{SiteConf, testing::TestSite};

fn test_conf() -> SiteConf {
    SiteConf::default()
}

#[vyuh::test(conf = test_conf)]
async fn configured_fixture(site: &TestSite) -> Result<(), ()> {
    let _ = site;
    Ok(())
}

fn main() {}
