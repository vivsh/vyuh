use vyuh::{SiteConf, bundles::Bundle, testing::TestSite};

fn test_conf() -> SiteConf {
    SiteConf::default()
}

fn app_bundle() -> Bundle {
    Bundle::default()
}

#[vyuh::test(conf = test_conf, bundle = app_bundle)]
async fn configured_bundle(site: &TestSite) -> Result<(), ()> {
    let _ = site;
    Ok(())
}

fn main() {}
