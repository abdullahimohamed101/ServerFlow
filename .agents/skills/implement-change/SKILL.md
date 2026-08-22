---
name: implement-change
description: Implement an approved execution plan or narrowly scoped engineering change.
---

# Implement Change

Before modifying code:

1. Read the execution plan.
2. Read relevant architecture documentation.
3. Confirm the current repository state.
4. Inspect existing patterns before introducing new abstractions.

During implementation:

- Keep changes scoped to the stated outcome.
- Preserve architectural boundaries.
- Reuse existing abstractions.
- Avoid unrelated cleanup.
- Do not introduce dependencies without justification.
- Add tests for changed behavior.

Before completion:

1. run focused tests
2. run relevant build/typecheck/lint commands
3. inspect diagnostics
4. inspect git diff
5. run git diff --check

Report:

- files changed
- behavior implemented
- commands executed
- test results
- design deviations
- unresolved risks
