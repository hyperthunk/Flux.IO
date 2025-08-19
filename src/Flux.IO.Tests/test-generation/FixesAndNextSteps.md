Tier‑B establishes structural guarantees and abstraction surfaces that Tier‑C assumes. If we short‑cut certain Tier‑B items, Tier‑C becomes either more complex or requires rework.

Below is a structured dependency map.

Core Abstraction & Interface Dependencies
Tier‑B Provides:

IJsonAssembler<JToken> abstraction (already in place).
ParseMode discriminated union and pipeline builder switch.
Structural framing (FramingAssembler) that removes reliance on expectedLength.
Internal streaming scanner (StreamingMaterializeAssembler) proving incremental scan feasibility.
Tier‑C Requires:

A stable boundary for swapping the “materializing” assembler with a token event reader (TokenStreamReader).
Confidence that chunk pipelines no longer depend on prior length knowledge (framer must be validated & optionally default).
Error and completion semantics (root termination detection) consistent across all assemblers.
Dependency: If the ParseMode switch and assembler interface are not frozen, adding PStream (token pipeline) risks churn. Finalize the assembler interface now (e.g. whether a future finalize/end-of-input signal will be introduced) before emitting external token streams.

Structural Framing vs External Token Streaming
What Tier‑B Guarantees:

FramingAssembler correctly identifies root completion without length.
Depth tracking logic proven via tests (instrumented assembler).
Needed by Tier‑C:

Token reader uses the same root completion semantics (same definition of “root terminal” to avoid divergence).
Depth and string escape rules verified (so token depth invariants in Tier‑C aren’t rediscovering bugs).
Dependency: If framing invariants (depth convergence, balanced containers, no early completion) are not fully locked, Tier‑C token depth invariants will generate duplicate failures or false positives. Complete and harden framing tests—including deep nesting and edge string cases—before broadening to token pipelines.

Completion & Finalization Semantics
Current Situation:

StreamingMaterializeAssembler scans incrementally but never receives a “final block” signal (isFinalBlock always false).
Corruption tests tolerate “pending” (neither complete nor error) for incomplete/truncated documents.
Tier‑C Needs:

A deterministic end-of-input signal to:
Emit a definitive error for truncated documents.
Allow token pipelines to raise final invariants (e.g. root must have closed by finalize).
Prevent indefinite Consume loops in live streaming scenarios.
Dependency: Introduce a finalize/end-of-input stage or a FeedEnd()/Flush() semantic (or add a zero-length feed with a flag) before exposing token streaming externally. Otherwise tests for malformed/truncated token streams will be fuzzy or permissive.

Instrumentation & Invariants
Tier‑B Instrumentation:

Depth trace, start/end object/array counts, primitive root detection.
Tier‑C Reuse:

Token-level invariants: depth never negative, exactly one root terminal, container open/close parity.
Streaming accumulation must rely on correct detection of root-level property/value pairing (depends on reliable token ordering from reader).
Dependency: Stabilize instrumentation data structures now (names, counters) so Tier‑C can lift them directly into runtime state or test harness without renaming churn.

Accumulation / Projection Semantics
Current (Tier‑B):

Accumulation & projection operate over final JToken.
Assumptions: single emission JToken; root object keys discoverable in one pass.
Tier‑C Goal:

Incrementally surface partial domain: root-level scalar keys (and optionally nested structures) before full materialization.
Dependencies:

Define canonical ordering for streaming accumulation (e.g. property name then its value token).
Decide monotonic exposure rule (emit each scalar once vs batching).
Align “done” condition (root closed OR all scalar names encountered earlier).
If not specified now, initial Tier‑C accumulation might mismatch expectations set by existing batch-based accumulation (e.g. thresholds vs single-key emission) and require translation glue or parallel modes.

Benchmark / Performance Baseline
Optional Tier‑B Step:

Running a baseline benchmark (LengthGate vs Frame vs StreamingMaterialize) BEFORE building PStream.
Dependency Nature: Not strictly blocking, but without a baseline you cannot attribute overhead introduced in Tier‑C to token emission vs pre-existing streaming scan costs. Capturing memory + throughput now avoids false regressions later.

Default ParseMode Decision
Current:

LengthGate is still the conservative default.
Tier‑C Impact:

If downstream test harnesses or external consumers migrate to streaming, they will expect length-independence everywhere.
Leaving LengthGate default invites silent assumptions (e.g. tests that still rely on expectedLength logic).
Dependency: Switching default to Frame before Tier‑C reduces risk that token streaming mode “works on my machine” but fails for long-tail cases where length preview had masked framing issues.

Error Surface Policy
Tier‑B:

Corruption tests currently allow a “pending” state rather than requiring an error without a final signal.
Tier‑C:

Token clients must distinguish: (a) well‑formed JSON ended; (b) more bytes required; (c) irrecoverable malformed input.
Dependency: Define a tri-state explicitly (NeedMore | Complete | Error) + finalize semantics so external streaming clients can implement backpressure and termination. Without finalize, token accumulators might wait forever on truncated feeds.

Testing Infrastructure Dependency
Machine Extensions:

Adding PStream requires either:
Extending DocumentRuntime with a StreamingDoc variant, OR
Parallel dictionary (as hinted) → unify later.
Dependency: Finalize how commands like StepToken, StepStreamAccum integrate with existing scheduling before implementing streaming invariants. If the machine refactor is postponed until after ad‑hoc token properties, you risk duplicating invariant logic.

Recommendation: Introduce a thin StreamingRuntime record + new Command union cases before implementing streaming accumulation, so invariants integrate once.

Summary Dependency Matrix
Tier‑B Item	Tier‑C Impact	Priority Before Tier‑C
Stable IJsonAssembler interface	Direct plug-in for TokenReader	MUST
Framing correctness + parity tests	Token depth & root boundary reliability	MUST
Finalize / end‑of‑input signal design	Deterministic malformed handling	HIGH
Instrumentation schema (depth counters, root flags)	Reused invariants & debugging	HIGH
Default ParseMode switch (Frame)	Length independence assumption	HIGH (optional but recommended)
Benchmark baseline	Performance regression attribution	MEDIUM
Streaming accumulation semantics spec	Avoid rework of domain events	HIGH
Materializer approach decision (token → JToken)	Parity & regression tests	MEDIUM
Corruption strictness policy	Negative test determinism	MEDIUM (depends on finalize)
Machine PStream command integration design	Unified model invariants	MUST (design)
Recommended Sequence Now

Decide finalize semantics (e.g. new method Finalize() on assembler or a sentinel Feed call with a flag).

Switch default to Frame (run all invariants again).

Lock instrumentation structure & naming.

Write a one‑page “Streaming Accumulation Spec” (event ordering, threshold adaptation, domain completeness rule).

Capture performance baseline (optional but valuable).

Implement PStream DU + minimal token reader integration (no accumulation yet) + machine support & depth invariants.

Add streaming accumulation & projection stages using agreed spec.

Add real token-based materializer (JTokenWriter) and parity property (DeepEquals).

Tighten corruption property (pending state disallowed after finalize).

Layer performance regression property or benchmark CI gate if needed.

When You Can Safely Start Tier‑C You can start once:
Framing tests are stable and green across fuzz partitions.
StreamingMaterializeAssembler (or its replacement) exposes or is paired with a finalization path.
A clear streaming accumulation spec is written (even if not fully implemented).
Starting earlier is possible but increases rework probability (particularly around finalize and accumulation semantics).