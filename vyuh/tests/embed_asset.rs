extern crate vyuh as renamed_vyuh;

use renamed_vyuh::embed::{self, Dir};
use renamed_vyuh::prelude::embed_assets;

const DEFAULT_ASSETS: Dir = embed::embed_assets!("web");
const FORCED_ASSETS: Dir = embed::embed_assets!("web", force = true);
const FORWARDED_ASSETS: Dir = embed::embed_assets!("web", force = false);
const PRELUDE_ASSETS: Dir = embed_assets!("web");

/// Verifies that every public macro form returns a usable constant asset directory.
#[test]
fn embed_asset_forms_create_dirs() {
    assert!(
        DEFAULT_ASSETS
            .get_file("public/css/manifest.json")
            .is_some()
    );
    assert!(FORCED_ASSETS.get_file("public/css/manifest.json").is_some());
    assert!(
        PRELUDE_ASSETS
            .get_file("public/css/manifest.json")
            .is_some()
    );
    assert!(
        FORWARDED_ASSETS
            .get_file("public/css/manifest.json")
            .is_some()
    );
    assert_eq!(DEFAULT_ASSETS.is_embedded(), FORWARDED_ASSETS.is_embedded());
}

/// Verifies that a forced asset directory is embedded independently of build mode.
#[test]
fn forced_assets_are_embedded() {
    assert!(FORCED_ASSETS.is_embedded());
}
