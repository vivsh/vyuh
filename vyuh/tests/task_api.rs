/// Verifies task registration rejects handler return values that carry data.
#[test]
fn task_handlers_are_value_less() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/tasks/macro_data_return.rs");
    tests.compile_fail("tests/ui/tasks/macro_result_data_return.rs");
    tests.compile_fail("tests/ui/tasks/direct_data_return.rs");
    tests.compile_fail("tests/ui/tasks/direct_result_data_return.rs");
}

/// Verifies macro and direct registration accept value-only batch handlers.
#[test]
fn task_batch_handlers_compile() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/tasks/batch_handlers.rs");
}
