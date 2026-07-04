# Agent Instructions

This file is for coding agents working in the Vyuh repository.

## Required Reading

- Read [CONTRIBUTING.md](CONTRIBUTING.md) before changing code.
- Read [ARCHITECTURE.md](ARCHITECTURE.md) before changing subsystem boundaries,
  feature flags, runtime wiring, macros, database code, tasks, emitters, or
  routing.

## Working Rules

- Keep changes scoped to the user request.
- Prefer existing subsystem patterns over new local abstractions.
- Do not make large scale, destructive, or multi-file changes without
  clarifying when the intent is ambiguous.
- Do not revert user changes or unrelated dirty work.
- Keep Postgres-first behavior explicit, and keep backend-specific code behind
  the correct backend cfg boundaries.
- Use `rg` for searching when available.
- Use `cargo check`, `cargo fmt`, and focused tests when the change touches
  Rust code.

## Documentation Rules

- Update [ARCHITECTURE.md](ARCHITECTURE.md) when changing major module
  ownership, backend feature boundaries, request flow, or subsystem wiring.
- Update [CONTRIBUTING.md](CONTRIBUTING.md) when coding conventions change.
- Keep subsystem documentation under `docs/book/src/` as book chapters. Do not
  add new top-level subsystem docs under `docs/`.
- Treat macros as sugar over direct APIs in docs. When a subsystem has macros,
  document the macro path and equivalent code registration together.

## Review Focus

- Check for accidental backend coupling.
- Check for panics in production paths.
- Check for new public APIs without explicit error behavior.
- Check that tests cover non-trivial behavior.
