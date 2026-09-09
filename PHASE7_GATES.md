# Gates: Phase 7 bulk replay and recovery rate limiting

Scope: Replay one partition range, independently pace Recovery publication and Recovery Store entry, and measure real Kafka/MySQL outcomes. No checkpoint, automatic resume, multi-partition scheduler, distributed limiter or Phase 8 feature.

- [x] B: Format, test, vet, build and Phase 7 tagged compile pass
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase7.ps1 -Check Build
  EXPECT: PHASE7_BUILD_PASS
  EVIDENCE: exit=0; Phase 0-6 build chain, unit tests, module verify, Phase 7 tagged compile/vet passed.

- [x] T: Existing Retry, DLQ and Recovery topics remain explicitly configured
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase7.ps1 -Check Topics
  EXPECT: PHASE7_TOPICS_PASS
  EVIDENCE: exit=0; explicit topic creation/description passed and automatic creation remains disabled.

- [x] S: Unlimited, publication-limited, backlog processing-limited, bulk duplicate, single replay and partial failure scenarios pass
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase7.ps1 -Check Scenarios
  EXPECT: PHASE7_SCENARIOS_PASS
  EVIDENCE: exit=0; six isolated Kafka/MySQL scenarios passed. Comparison cells each reconciled 120+0+0=120 and inventory 12000->11880.

- [x] R: Preserved Phase 5/6 evidence and dependency scope pass without Phase 4 performance rerun
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase7.ps1 -Check Regression
  EXPECT: PHASE7_REGRESSION_PASS
  EVIDENCE: exit=0; Phase 6 evidence and no-unexpected-dependency checks passed.

- [x] A: Raw execution files reconcile and generate Phase 7 evidence with source hashes
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase7.ps1 -Check Audit
  EXPECT: PHASE7_AUDIT_PASS
  EVIDENCE: exit=0; latest PASS raw exists for all six strategies and three comparison cells reconcile.

- [x] D: Verification, evidence, eleven-section implementation document and README report actual measurements and limits

- [x] C: Secret-free scoped changes committed locally and working tree clean; no push
