# Gates: Phase 1 normal order inventory flow

OWNS: cmd/api/**, cmd/worker/**, cmd/smoke/mysql.go, internal/**, migrations/**, scripts/**, tests/integration/**, .env.example, README.md, docs/phase-1-*, PHASE1_GATES.md

Scope: Normal HTTP to Kafka consumer-group to MySQL flow only. Phase 0 baseline commit 856f76148bc864084c1e6b57e5ccd5eadf89fdd3 is preserved. No Phase 2 recovery or idempotency.

- [x] P0: Phase 0 baseline is committed with secrets excluded and a clean working tree
  EVIDENCE: git commit 856f76148bc864084c1e6b57e5ccd5eadf89fdd3; message chore: set up phase 0 development environment; exact 15-file allowlist and password exclusion passed; staged diff check passed after removing EOF blank lines; git status returned only main.

- [x] P1: All Go code is formatted, unit tests and vet pass, and programs build without new modules
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase1.ps1 -Check Build
  EXPECT: PHASE1_BUILD_PASS
  EVIDENCE: exit=0; shell=cmd.exe; cwd=project root; path=8e091ab82072/45 entries; EXPECT=matched; output-sha256=f315771c0aa409ddea8d231d005ca20d1b55d81e05b1e1f27ec881e2487fc23b; output-bytes=563

- [x] P2: Phase 0 infrastructure, topic, Kafka/MySQL smoke and persistence checks remain successful
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase1.ps1 -Check Phase0
  EXPECT: PHASE0_REGRESSION_PASS
  EVIDENCE: exit=0; shell=cmd.exe; cwd=project root; path=8e091ab82072/45 entries; EXPECT=matched; output-sha256=7165709564924722f2ee3c91efb657cd41af1506799c8fe678a1184cbbf25a10; output-bytes=3557

- [x] P3: Actual API and worker processes pass stock arithmetic, broker offset and graceful process-restart checks; invalid HTTP publishes nothing
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase1.ps1 -Check Flow
  EXPECT: PHASE1_FLOW_PASS
  EVIDENCE: exit=0; shell=cmd.exe; cwd=project root; path=8e091ab82072/45 entries; EXPECT=matched; output-sha256=51d2d054ed5073a5f7b2231c6664fae9c468426f07037b0d3b3a49a9a59be43d; output-bytes=1692

- [x] P4: Source, live topic/schema lists and module graph contain only authorized Phase 1 features
  EVIDENCE: manual source review plus live SHOW TABLES returned inventory,phase0_probe; topic list returned only orders.created.v1,phase0.smoke.v1,__consumer_offsets; go.mod/go.sum/compose.yaml unchanged; no retry, DLQ, idempotency, replay, extra framework or infrastructure. Exactly 24 changed files (5 modified,19 new) matched the allowlist.

- [x] P5: Reproducible instructions and actual event, stock and committed-offset evidence are recorded without secrets
  EVIDENCE: README and 19-section phase-1 report reviewed against request; evidence JSON records actual 7 accepted events, quantity sum 19, stock 100 to 81, 5 rejected inputs, broker offsets and PID 3052 to 33184 with 0 restarted processing; earlier normal record distinguished from final run; exact local-password comparison across all 24 changes passed; .env and bin ignored.

- [x] P6: Phase 1 completion commit contains only reviewed files and leaves a clean working tree after all acceptance checks pass
  EVIDENCE: local commit with message feat: implement phase 1 order inventory flow succeeded after P0-P5, exact 24-path staged allowlist, staged password exclusion and git diff --cached --check; git status returned only main. This completion evidence and report wording are folded into that same unpushed commit without code changes; final hash is obtained from git log.
