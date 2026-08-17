//! Authenticated endpoint-scoped signal subscriptions.
//!
//! Run with `cargo run -p vyuh --example beacon`, then authenticate with the
//! development provider and request `/live?transport=poll`.

use std::time::Duration;

use schemars::JsonSchema;
use vyuh::{
    auth::{Audience, AuthConf},
    prelude::*,
};

const NOTES: Audience = Audience::new("notes");

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct NoteChanged {
    owner: String,
    note_id: i64,
}

#[bundles::beacon(path = "/live", modes = [ws, sse, poll])]
fn live_notes() -> Beacon {
    Beacon::builder()
        .rule_with::<NoteChanged>(["notes:read"], |user, note| note.owner == user.subject())
        .debounce::<NoteChanged>(Duration::from_millis(150))
        .build()
}

#[bundles::route(path = "/notes/{owner}/{note_id}", method = "POST")]
async fn change_note(site: Site, Path((owner, note_id)): Path<(String, i64)>) -> StatusCode {
    let _ = site.signals().emit(NoteChanged { owner, note_id });
    StatusCode::ACCEPTED
}

fn app_bundle() -> bundles::Bundle {
    bundles::bundle! { live_notes, change_note }
        .with_conf(bundles::conf().audience(NOTES).tags(["notes"]))
}

#[tokio::main]
async fn main() -> Result<(), SiteError> {
    Site::serve(
        SiteConf::default()
            .host("127.0.0.1")
            .auth(AuthConf::development()),
        app_bundle(),
    )
    .await
}
