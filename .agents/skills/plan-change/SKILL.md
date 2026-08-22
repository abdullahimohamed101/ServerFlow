---
name: plan-change
description: Plan a non-trivial feature, bug fix, migration, refactor, or architectural change before implementation.
---

# Plan Change

Do not modify production code while executing this skill.

## Process

1. Restate the intended user or system outcome.
2. Identify non-goals.
3. Inspect the existing implementation and architecture.
4. Find existing patterns that should be reused.
5. Identify affected modules, APIs, schemas and dependencies.
6. Identify unknowns.
7. Investigate important unknowns before proposing implementation.
8. Define explicit acceptance criteria.
9. Define a verification strategy.
10. Identify security, reliability, migration and compatibility risks.
11. Identify which work can proceed independently.
12. Produce the smallest coherent implementation plan.

## Output

Produce:

- Outcome
- Non-goals
- Current architecture
- Proposed design
- Affected files/components
- Acceptance criteria
- Verification plan
- Risks
- Ordered implementation steps

For substantial work, persist the result under docs/plans/active/.S
