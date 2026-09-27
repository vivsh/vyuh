/// Extractor Data wrappers are not output values; direct serializable outputs use no wrapper.
#[test]
fn task_outputs_are_not_extractors() {
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

/// Verifies spawn requests are constructed by handlers, not the site task facade.
#[test]
fn task_spawn_is_outcome_only() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/tasks/spawn_handlers.rs");
    tests.compile_fail("tests/ui/tasks/facade_spawn_unavailable.rs");
}

/// Registration enforces synchronous Flow capabilities without inspecting type syntax.
#[test]
fn work_flow_capabilities() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/tasks/flow_handlers.rs");
    tests.compile_fail("tests/ui/tasks/flow_invalid_*.rs");
    tests.compile_fail("tests/ui/tasks/work_invalid_*.rs");
    tests.pass("tests/ui/tasks/work_continuation.rs");
    tests.pass("tests/ui/tasks/typed_outputs.rs");
    tests.compile_fail("tests/ui/tasks/typed_invalid_*.rs");
}
