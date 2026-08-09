/// Verifies the removed numeric-role authorization surface has no compatibility aliases.
#[test]
fn role_based_authentication_apis_are_removed() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/auth/bitrole.rs");
    tests.compile_fail("tests/ui/auth/permit_macro.rs");
    tests.compile_fail("tests/ui/auth/role_builders.rs");
    tests.compile_fail("tests/ui/auth/provider_kind.rs");
}

/// Verifies superseded OAuth and OIDC names have no compatibility aliases.
#[cfg(all(feature = "oauth", feature = "federated"))]
#[test]
fn superseded_external_authentication_names_are_removed() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/auth/oauth_access.rs");
    tests.compile_fail("tests/ui/auth/oidc_login.rs");
}
