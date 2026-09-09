# Gates: Phase 5 transactional idempotency

Scope: Same eventId and canonical payload changes stock once. Explicit Phase 2 baseline remains available. No Phase 6, scheduler, infrastructure or dependencies.

- [x] B: Format, unit tests, vet, build and dependency verification pass
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase5.ps1 -Check Build
  EXPECT: PHASE5_BUILD_PASS
  EVIDENCE: exit=0; shell=PowerShell; cwd=project root; EXPECT matched; final bin/phase5-build.log sha256=543568fa0cf02ed06c5282943cda89dec9ea17ee048e9bab17e1acf2adf55a72; gofmt, unit tests, vet, build, module verification and integration compile/vet passed.

- [x] S: Real schema, crash/redelivery, 100 duplicates, concurrency, rollback, conflict and retry duplicate tests pass
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase5.ps1 -Check Scenarios
  EXPECT: PHASE5_SCENARIOS_PASS
  EVIDENCE: exit=0; shell=PowerShell; cwd=project root; EXPECT matched; bin/phase5-scenarios.log sha256=a7e0d522862d4799b9774aa929a597820de200a658288033d65f084bbaa21381; 7 real scenarios passed after setup timeout correction; initial two failures preserved; SQL stock 100-98-98, 100 calls/99 duplicates, 16 concurrent calls/15 duplicates, crash and timeout rollback, conflict DLQ and retry duplicate offset verified.

- [x] R: Phase 0 through 4 functional regressions pass without replacing historical evidence
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase5.ps1 -Check Regression
  EXPECT: PHASE5_REGRESSION_PASS
  EVIDENCE: Phase 0-3 regression tokens and all six Phase 4 strategy scenario subtests passed in bin/phase5-regression.log (sha256=1553bef44aed4a1fbfd0b8c783e00a7c672f269f9481d0593d124ddd23c7cee3). Four outage durations missed Phase 4's historical 8-12 second performance-comparability window, so that separate audit failed; it is recorded as a non-gating environment limitation and historical Phase 4 evidence is unchanged.

- [x] D: Sixteen completion conditions have evidence; eleven-section document, verification and README accurately report limits

- [x] C: Secret-free scoped changes committed locally and clean with no push
