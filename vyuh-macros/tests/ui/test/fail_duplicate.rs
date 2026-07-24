use vyuh::{SiteConf, testing::TestSite};

#[vyuh::test(conf = SiteConf::default(), conf = SiteConf::default())]
async fn duplicate_option(site: &TestSite) -> Result<(), ()> {
    let _ = site;
    Ok(())
}

fn main() {}
