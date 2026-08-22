---
name: verify-change
description: Independently verify an implementation against its acceptance criteria and look for regressions or missing coverage.
---

# Verify Change

Act as an independent verification engineer.

Do not assume the implementation author's reasoning is correct.

## Process

1. Read requirements and acceptance criteria first.
2. Derive expected behavior independently.
3. Inspect the resulting implementation.
4. Run focused tests.
5. Run relevant integration tests.
6. Run lint/typecheck/build where applicable.
7. Exercise failure paths and edge cases.
8. Look for regressions.
9. Identify missing test coverage.
10. Distinguish verified behavior from assumptions.

## Output

### Verified
Evidence-backed behavior that works.

### Failed
Behavior that does not satisfy requirements.

### Missing Coverage
Important behavior that is not adequately tested.

### Unverified
Anything that could not be conclusively checked.

Include exact commands and results.
