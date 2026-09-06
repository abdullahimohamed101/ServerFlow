# Agent Engineering Guide

This file is the entry point for AI agents working in this repository.
Keep it concise. Detailed architecture and product knowledge belongs in /docs.

## Mission

Produce correct, maintainable, testable software that satisfies the stated
requirements while preserving the architecture of the repository.

Correctness and maintainability take priority over minimizing lines changed.

## Before Making Changes

1. Understand the requested outcome.
2. Inspect the relevant existing implementation.
3. Read ARCHITECTURE.md and relevant documentation.
4. Identify affected boundaries and dependencies.
5. Determine how the change will be verified.
6. For non-trivial work, create or read the execution plan before editing code.

Do not begin a large implementation when important requirements remain ambiguous.

## Engineering Principles

- Prefer the smallest coherent solution.
- Do not refactor unrelated code.
- Reuse existing abstractions before creating new ones.
- Avoid unnecessary dependencies.
- Validate external data at system boundaries.
- Preserve established module and layering boundaries.
- Prefer explicit behavior over hidden magic.
- Optimize for code that future engineers and agents can understand.

## Testing

Behavior changes require appropriate verification.

After modifying code:

1. Run focused tests.
2. Run relevant integration tests when available.
3. Run type checking, linting and build validation when applicable.
4. Inspect diagnostics.
5. Report anything that could not be verified.

Never remove, weaken, skip or rewrite a failing test merely to make validation pass.

## Debugging

Do not blindly patch symptoms.

When debugging:

1. reproduce the failure
2. gather evidence
3. identify the root cause
4. implement the smallest correct fix
5. prove the original failure is resolved
6. check for regressions

## Git Safety

- Never force push.
- Never push directly to main.
- Never discard existing uncommitted user changes.
- Never run destructive Git commands without explicit approval.
- Do not commit unless requested or required by an approved workflow.
- Do not push unless explicitly approved.

## Security

Never expose, print, modify or commit:

- API keys
- passwords
- access tokens
- private keys
- .env secrets

Security-sensitive changes require explicit security review.

## Completion Standard

A task is not complete merely because code was generated.

Completion requires evidence:

- requirements satisfied
- relevant tests pass
- diagnostics are clean
- changes were reviewed
- known risks are documented

## Repository Knowledge

Source of truth (read first):
See docs/architecture/serverflow-spec.md

Architecture:
See ARCHITECTURE.md

Active execution plans:
See docs/plans/active/

Completed execution plans:
See docs/plans/completed/

Security:
See docs/SECURITY.md when present.

Reliability:
See docs/RELIABILITY.md when present.
