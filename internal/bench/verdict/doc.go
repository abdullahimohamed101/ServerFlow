// Package verdict evaluates pre-registered experiment predictions mechanically (ADR-022, PR G-lite;
// Phase 14 D25 and D26). It reads a predictions file, a freeze record and the machine-readable result files
// of the runs, and prints one verdict per hypothesis: confirmed, refuted, inconclusive or invalid. It never
// edits a prediction and never reads a clock: the same inputs give the same bytes.
//
// Every number it prints is a simulator number when the runs are simulator runs: simulator-validated, not
// hardware-validated.
//
// # Statistics
//
// Runs are paired by (seed, repeat index): the same workload offered to both arms. For a metric x (for
// example p95 latency) with baseline values a_i and treatment values b_i the per-pair relative difference is
//
//	r_i = (b_i - a_i) / a_i          (a_i must be positive and finite)
//
// and the summary is the median of the r_i. For a superiority claim the sign is chosen so that a positive
// number is an improvement in the claimed direction (the "effect": e_i = -r_i when the claim is that B is
// lower, e_i = r_i when higher).
//
// Interval: the percentile bootstrap. Resample the n pairs with replacement B times (default 10,000), take the
// median of each resample, sort the B medians and read the quantiles alpha/2 and 1-alpha/2 (alpha = 0.05)
// by linear interpolation between order statistics (Hyndman and Fan type 7). The resampling stream is
// math/rand/v2's PCG seeded with (seed, constant), so the interval is a pure function of the data, B and the seed.
//
// P-value for H0 "the median is zero": two-sided, from the same replicates,
//
//	p_boot = min(1, 2 * min( (1 + #{rep <= 0}) / (B + 1), (1 + #{rep >= 0}) / (B + 1) ))
//
// The added 1 keeps p above zero (a bootstrap cannot show p = 0). A bootstrap p-value is anti-conservative at
// small n: five all-positive pairs give p_boot = 2/(B+1), about 0.0002, although the exact sign test on five pairs
// cannot give less than 2/2^5 = 0.0625. The p-value a verdict uses is therefore
//
//	p = max( p_boot, 2 / 2^n )          (n = number of non-zero pairs; capped at 1)
//
// so it is never smaller than what an exact sign test could support. It is the value Holm adjusts and the value
// confirmation is judged on (confirmation also needs the 95% interval to exclude zero, and the median to reach
// the threshold). The floor is 0.0625 for n = 5, 0.03125 for n = 6, 0.0156 for n = 7, 0.0078 for n = 8.
// Consequence, stated plainly: with alpha = 0.05 a superiority claim can never be CONFIRMED from 5 pairs, needs at
// least 6 non-zero pairs on its own, and at least 7 when two confirmatory claims share the Holm family (2 * 0.0156 =
// 0.031; with 6 pairs 2 * 0.03125 = 0.0625). Refutation and equivalence do not use the p-value and are unaffected.
//
// Holm: for m confirmatory p-values sorted ascending p_(1) <= ... <= p_(m), the adjusted values are
//
//	adj_(i) = min(1, max_{j <= i} (m - j + 1) * p_(j))
//
// and a hypothesis is rejected when adj < 0.05 (strictly). It controls the family-wise error rate for any
// dependence between the tests. The family is every confirmatory hypothesis declared in the predictions file,
// whether or not its runs exist: a hypothesis without data counts with p = 1, so missing data can never make
// another confirmatory claim easier to confirm.
//
// Equivalence (H2, H11): the TOST idea in its interval form, as the plan fixes it: equivalent only if the 95%
// bootstrap interval of the median paired relative difference lies entirely inside (-margin, +margin). That
// is the two one-sided tests at alpha = 0.025 each. The package also computes the parametric t-based TOST
// (two one-sided t-tests on the mean of the r_i, same margin, alpha = 0.025) and prints it as a descriptive
// cross-check; it never decides a verdict.
//
// Degenerate data: if every pair has exactly the same value (zero spread: identical repeats, a constant
// improvement) the bootstrap interval has no width and the median is exact by construction. Such a cell is
// INCONCLUSIVE with the reason "degenerate: zero spread", for superiority and equivalence alike, never CONFIRMED
// and never REFUTED.
//
// Independence: pairs are formed by (seed, repeat index). Repeats of one deterministic simulated seed are not
// independent samples of the workload: they share the request stream, and only the timing differs. A cell backed
// by fewer than 3 distinct seeds carries a warning (the verdict does not change); the registered seed list, not
// the number of repeats, is what makes a cell evidence about a workload.
//
// What the verdicts mean: a statement about the simulated worlds and workloads in the result files, under the
// seeds in the predictions. Nothing here supports a claim about real hardware.
package verdict
