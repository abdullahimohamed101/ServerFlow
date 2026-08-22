---
name: harden-change
description: Improve an already-correct implementation for maintainability, simplicity, reliability and architectural quality without changing intended behavior.
---

# Harden Change

Only run after behavior has been verified.

Look for:

- unnecessary abstractions
- duplicated logic
- confusing naming
- dead code
- oversized functions
- weak type boundaries
- poor error handling
- resource leaks
- avoidable complexity
- architectural drift
- performance traps

Prefer simplification over abstraction.

Do not expand feature scope.

After changes:

1. rerun all relevant verification
2. compare behavior before and after
3. inspect the final diff
