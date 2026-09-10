# Gates: Phase 8 Prometheus and Grafana observability

Scope: Expose bounded Worker metrics, scrape them from local Prometheus, provision a reproducible Grafana dashboard, and compare real metric deltas with Kafka/MySQL state. No alerting, tracing, log aggregation, exporter, Pushgateway, checkpoint or Phase 9 feature.

- [x] B: Format, test, vet, build, module verify and Phase 8 tagged compile pass
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase8.ps1 -Check Build
  EXPECT: PHASE8_BUILD_PASS
  EVIDENCE: exit=0; all Go packages and Phase 8 integration compile/vet passed.

- [x] I: Prometheus and Grafana configuration, health and provisioning pass
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase8.ps1 -Check Infrastructure
  EXPECT: PHASE8_INFRASTRUCTURE_PASS
  EVIDENCE: exit=0; promtool config check, four healthy Compose services, datasource and nine-panel dashboard passed.

- [x] S: Real Normal, Retry, DLQ, Duplicate, Recovery and small rate-limit metrics match Kafka/MySQL state
  CHECK: go test '-tags=integration,phase2,phase3,phase4,phase5,phase6,phase7,phase8' -count=1 -timeout=6m -run '^TestPhase8$' -v ./tests/integration
  EXPECT: TestPhase8 PASS
  EVIDENCE: exit=0; nine Prometheus before/after observations, three UP targets, DB state, offsets and ten Grafana queries passed.

- [x] R: Affected Retry, idempotency, replay, rate limit and preserved Phase 4-7 evidence pass
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase8.ps1 -Check Regression
  EXPECT: PHASE8_REGRESSION_PASS
  EVIDENCE: exit=0; affected unit tests and preserved evidence checks passed without Phase 4/7 performance rerun.

- [x] A: PASS raw, failed raw list and SHA-256 generate consistent Phase 8 evidence
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase8.ps1 -Check Audit
  EXPECT: PHASE8_AUDIT_PASS
  EVIDENCE: exit=0; one PASS execution selected and four failed executions retained.

- [x] C: Cardinality audit exposes only bounded label names and values
  EVIDENCE: error_code, le, outcome, strategy and worker only; forbidden identifiers and dynamic topics absent.

- [x] D: Verification, evidence, eleven-section document and README report actual results and limits

- [x] G: Secret-free scoped changes committed locally and working tree clean; no push
