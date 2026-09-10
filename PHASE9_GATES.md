# Gates: Phase 9 repeated experiment validation

Scope: Repeat only the Phase 4 six-cell Retry comparison and Phase 7 three-cell Replay/Recovery comparison three times, preserve every run, aggregate median/min/max, and limit claims to the local n=3 evidence. No Phase 10 feature.

- [x] E: Environment captured once before repetition
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase9.ps1 -Check Environment
  EXPECT: PHASE9_ENVIRONMENT_PASS
  EVIDENCE: Go 1.26.5, Kafka 4.2.0, MySQL 8.4.8, Docker 29.2.0, Windows amd64, 12 logical CPUs and experiment controls recorded.

- [x] P4: Six Phase 4 cells each have exactly three independent valid runs
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase9.ps1 -Check Phase4
  EXPECT: PHASE9_PHASE4_PASS
  EVIDENCE: 18 PASS raw files; three deterministic seed pairs; actual outage 9.701..11.166 seconds retained without result filtering.

- [x] P7: Three Phase 7 cells each have exactly three independent valid runs
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase9.ps1 -Check Phase7
  EXPECT: PHASE9_PHASE7_PASS
  EVIDENCE: nine PASS raw files; every run reconciles 120 success, zero DLQ and zero unfinished with inventory, markers and committed offsets.

- [x] A: Raw files, SHA-256, repetition, controls and median/min/max aggregate consistently
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase9.ps1 -Check Audit
  EXPECT: PHASE9_AUDIT_PASS
  EVIDENCE: 27 valid runs, zero invalid runs, prior Phase 4/7 single results retained separately.

- [x] B: Format, test, vet, build, module verification and Phase 9 tagged compile pass
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase9.ps1 -Check Build
  EXPECT: PHASE9_BUILD_PASS
  EVIDENCE: exit=0 without rerunning Phase 0-8 integration scenarios.

- [x] D: Verification, evidence, eleven-section document and README report actual ranges and claim limits

- [x] S: No dependency, production feature, Phase 8 metric/dashboard, exact lag collector or Phase 10 change

- [x] G: Scoped secret-free changes committed locally; no push
