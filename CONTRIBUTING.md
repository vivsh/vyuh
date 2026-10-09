# Contributing

These rules apply to contributors and coding agents working in this repository.

## Hard Rules

- Functions must never exceed 50 lines. Prefer small, focused helpers.
- Treat 1,000 lines as a review trigger, not a hard file limit. Split by cohesive
  responsibility when that improves navigation; do not add forwarding layers or
  artificial modules solely to meet a line count.
- Keep `mod.rs` clean and tiny - it should be import/exports/re-exports/mod
  statements only except in special cases.
- Function names must be descriptive but no more than 4 words; prefer 1-2 word
  identifiers.
- Never include any code that can panic: do not use `panic!`, `unwrap()`,
  `expect()`, `unwrap_or_else()` patterns that may panic, or indexing
  (`arr[i]`) that can panic. All fallible operations must return `Result` or
  `Option` and be handled explicitly.
- Avoid unnecessary allocations. Prefer borrowing (`&str`, `&[T]`) and iterator
  adapters; pre-allocate (`Vec::with_capacity`, `reserve`) where repeated push
  is expected.

## Style And Safety

- Prefer `Result<T, E>` for fallible APIs; propagate errors with `?` and create
  small error types. Use `thiserror` where appropriate.
- Use `get()` when accessing collections by index and handle the `None` case
  rather than indexing.
- Prefer `&str` or `Cow<'a, str>` over `String` for inputs; return owned values
  only when necessary.
- Avoid `clone()` on large structures; prefer references or move semantics. If a
  clone is unavoidable, document why.
- Use explicit capacity hints for collections when size is known or can be
  estimated.

## Performance And Allocations

- Use iterator combinators and avoid intermediate collections when possible.
- Use `Iterator::map` plus `collect` only once when collecting is needed.
- Favor `BTreeMap`/`HashMap` entry API (`entry().or_insert_with`) to avoid
  double lookups.
- Prefer stacking small transformations over allocating temporary Vecs; use
  `filter_map` and `fold` to accumulate results without intermediate
  allocations.

## API And Naming

- Public types and functions should have short, meaningful names.
- Keep public names no longer than 4 words.
- Prefer concise nouns and verbs such as `validate`, `to_nested_map`, and
  `into_field_map`.
- Prefer `snake_case` for functions and variables.
- Prefer `CamelCase` for types and enums.
- Keep function signatures explicit and ergonomic: accept `&T` or
  `impl AsRef<str>` where appropriate; return owned values only when ownership
  is required.

## Error Handling

- Do not swallow errors silently.
- Return errors or convert them to a meaningful domain error.
- When converting to JSON or another external representation, map errors to
  stable, documented shapes.

## Testing And Correctness

- Add unit tests for any non-trivial logic.
- Test edge cases: empty inputs, large inputs, and deeply nested structures.
- Run `cargo check`, `cargo fmt`, and `cargo clippy` locally before committing
  suggestions.
- For backend-sensitive code, check each supported backend feature separately
  instead of using `--all-features`.

## Docs And Comments

- Add concise doc-comments (`///`) to public items explaining intent and
  invariants.
- For internal helpers, add a short comment when behavior or safety is
  non-obvious.
- Keep comments useful; do not restate the function name or obvious assignment.

## Forbidden Patterns

- No raw indexing (`arr[i]`) without `get()` checks.
- No `unwrap()` / `expect()` / `panic!` / `unwrap_unchecked`.
- No silent `unwrap_or_default()` that masks real errors without documentation.

## Notes For Suggestions

- Prefer safe, minimal allocations and explicit error returns.
- If a suggestion benefits from an allocation, include a short comment
  explaining why and whether it can be avoided.
- If a long helper is needed, split it into smaller functions so none exceed 50
  lines.

## Maintenance

- Keep these guidelines updated in the file root.
- If repository conventions change, update this file in the same change.
