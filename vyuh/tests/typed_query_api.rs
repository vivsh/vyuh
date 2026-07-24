#![cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]

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
    tests.compile_fail("tests/ui/typed_query/output_column_expr.rs");
    tests.compile_fail("tests/ui/typed_query/select_write.rs");
    tests.compile_fail("tests/ui/typed_query/select_wrong_type.rs");
    tests.compile_fail("tests/ui/typed_query/values_wrong_type.rs");
    tests.compile_fail("tests/ui/typed_query/window_wrong_type.rs");
    tests.compile_fail("tests/ui/typed_query/json_on_non_json.rs");
    tests.compile_fail("tests/ui/typed_query/array_on_non_array.rs");
    tests.compile_fail("tests/ui/typed_query/filter_missing_column.rs");
    tests.compile_fail("tests/ui/typed_query/filter_builder_order.rs");
    tests.compile_fail("tests/ui/typed_query/filter_wrong_operator.rs");
    tests.compile_fail("tests/ui/typed_query/old_filter_syntax.rs");
    tests.compile_fail("tests/ui/typed_query/old_reference_on_syntax.rs");
    tests.compile_fail("tests/ui/typed_query/old_table_columns_syntax.rs");
}
