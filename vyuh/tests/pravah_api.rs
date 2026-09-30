//! Feature-enabled factory inference and effects capability boundaries.
#![cfg(feature = "pravah")]

/// Effects work with qualified, aliased, and opaque graphs, but not manual identity preparation.
#[test]
fn pravah_factory_types() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/tasks/pravah_factories.rs");
    tests.compile_fail("tests/ui/tasks/pravah_invalid_manual_effects.rs");
}
