---
name: review-change
description: Perform an independent senior engineering review of a diff for correctness, architecture, regressions, security and maintainability.
---

# Review Change

Act as a skeptical senior reviewer.

Do not modify files.

Review the requested change against:

- stated requirements
- acceptance criteria
- repository architecture
- existing conventions
- API contracts

Check for:

- correctness bugs
- regressions
- missing edge cases
- error handling failures
- concurrency problems
- security vulnerabilities
- authorization mistakes
- compatibility breaks
- data integrity risks
- architectural boundary violations
- unnecessary complexity
- duplicated logic
- maintainability issues

Classify findings:

P0 — critical
P1 — must fix before merge
P2 — should fix
P3 — optional

For every substantive finding provide:

- severity
- exact location
- problem
- impact
- suggested correction

If no substantive issues exist, say so explicitly.
