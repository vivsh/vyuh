/// Verifies accepted and rejected `#[vyuh::test]` forms through downstream macro expansion.
#[test]
fn test_attribute_api() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/test/pass_default.rs");
    tests.pass("tests/ui/test/pass_conf.rs");
    tests.pass("tests/ui/test/pass_bundle.rs");
    tests.pass("tests/ui/test/pass_conf_bundle.rs");
    tests.pass("tests/ui/test/pass_without_migrations.rs");
    tests.compile_fail("tests/ui/test/fail_unknown.rs");
    tests.compile_fail("tests/ui/test/fail_duplicate.rs");
    tests.compile_fail("tests/ui/test/fail_migrations.rs");
    tests.compile_fail("tests/ui/test/fail_malformed.rs");
    tests.compile_fail("tests/ui/test/fail_not_async.rs");
    tests.compile_fail("tests/ui/test/fail_site.rs");
    tests.compile_fail("tests/ui/test/fail_result.rs");
}
