# Gates: Phase 6 DLQ replay and recovery

Scope: Select one DLQ record by topic/partition/offset, publish its original bytes to Recovery, and reuse the existing Retry/DLQ/idempotent inventory path. No bulk replay, rate limiting, scheduler or replay database.

- [x] B: Format, test, vet, build and Phase 6 tagged compile pass
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase6.ps1 -Check Build
  EXPECT: PHASE6_BUILD_PASS
  EVIDENCE: exit=0; Phase 0-5 build chain, unit tests, module verify, Phase 6 tagged compile/vet and replay CLI build passed.

- [x] T: Retry, DLQ and inventory.recovery.v1 topics exist with explicit one-partition configuration
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase6.ps1 -Check Topics
  EXPECT: PHASE6_TOPICS_PASS
  EVIDENCE: exit=0; broker create --if-not-exists and describe checks passed; automatic creation remains disabled.

- [x] S: Real exhaustion recovery, duplicate replay, recovery retry, recovery DLQ and replay failures pass
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase6.ps1 -Check Scenarios
  EXPECT: PHASE6_SCENARIOS_PASS
  EVIDENCE: exit=0; four Kafka/MySQL scenarios passed. Publication remained distinct from DB recovery; stock 100->98->98, marker 0->1->1, Recovery committed -1->1->2; retry restarted at one; failed Domain lineage produced replay counts one then two; malformed/publish failure produced no Recovery record.

- [x] R: Affected Retry/idempotency behavior and preserved Phase 4-5 evidence pass
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase6.ps1 -Check Regression
  EXPECT: PHASE6_REGRESSION_PASS
  EVIDENCE: exit=0 after correcting the checker to use the existing Phase 4 JSON key `Status`; inventory unit suite and preserved evidence checks passed. Phase 4 performance scenarios were not rerun.

- [x] D: Evidence, verification, eleven-section implementation document and README report measured results and limits

- [x] C: Secret-free scoped changes committed locally and working tree clean; no push
