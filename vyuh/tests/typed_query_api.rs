/// Verifies typed-query source/record/projection boundaries at compile time.
#[test]
fn typed_query_api_rejects_record_sources() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/typed_query/record_cols.rs");
    tests.compile_fail("tests/ui/typed_query/from_record.rs");
    tests.compile_fail("tests/ui/typed_query/cols_for.rs");
    tests.compile_fail("tests/ui/typed_query/source_column.rs");
    tests.compile_fail("tests/ui/typed_query/inspect.rs");
    tests.compile_fail("tests/ui/typed_query/terminal_filter.rs");
    tests.compile_fail("tests/ui/typed_query/write_cte.rs");
    tests.compile_fail("tests/ui/typed_query/top_level_exports.rs");
}
