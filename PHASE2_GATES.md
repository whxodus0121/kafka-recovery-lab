# Gates: Phase 2 deterministic failure reproduction

OWNS: internal/inventory/**, internal/fault/**, internal/kafka/client.go, cmd/worker/main.go, tests/integration/**, scripts/verify-phase2.ps1, README.md, docs/phase-2-*, PHASE2_GATES.md

Scope: Reproduce unresolved Kafka/MySQL failure boundaries from the earlier ticketing project. No repair features, offset resets, old-inventory resets or new dependencies. Baseline commit 0f4b730371998b0240aa922678589782fe14e7bd.

- [x] B1: Formatting, unit tests, vet, builds and unchanged modules pass with faults disabled
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase2.ps1 -Check Build
  EXPECT: PHASE2_BUILD_PASS
  EVIDENCE: exit=0; shell=cmd.exe; cwd=project root; path=900acd22c11c/45 entries; EXPECT=matched; output-sha256=80c39a5ed7b929888b38df2da3087c0dbdefdd8bd05c0362719cba27b5a7fb66; output-bytes=632

- [x] B2: Phase 0 regression and original Phase 1 normal flow pass without resetting historical inventory
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase2.ps1 -Check Regression
  EXPECT: PHASE2_REGRESSION_PASS
  EVIDENCE: exit=0; shell=cmd.exe; cwd=project root; path=900acd22c11c/45 entries; EXPECT=matched; output-sha256=6ea89660c1f709625726113341a795c2ac4ed6e32341a884445f0a9309b7df24; output-bytes=5443

- [x] S1: A real MySQL outage leaves the record uncommitted and the same coordinate succeeds after restoration
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase2.ps1 -Check A
  EXPECT: PHASE2_A_PASS
  EVIDENCE: exit=0; shell=cmd.exe; cwd=project root; path=900acd22c11c/45 entries; EXPECT=matched; output-sha256=ebac487ed87649313d153f1e1ceef7c41ab71af82ce888f5fef90f8dcb228a43; output-bytes=285

- [x] S2: Crash after UPDATE but before DB commit leaves no DB change and redelivers the same record
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase2.ps1 -Check B
  EXPECT: PHASE2_B_PASS
  EVIDENCE: exit=0; shell=cmd.exe; cwd=project root; path=900acd22c11c/45 entries; EXPECT=matched; output-sha256=dac84fdb717a52cb538ce7bfea6f7bf0685989f41255ee5433cd25040ff4c67d; output-bytes=281

- [x] S3: Crash after successful DB commit leaves Kafka uncommitted and redelivery duplicates the same event's DB effect
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase2.ps1 -Check C
  EXPECT: PHASE2_C_PASS
  EVIDENCE: exit=0; shell=cmd.exe; cwd=project root; path=900acd22c11c/45 entries; EXPECT=matched; output-sha256=49e1af7fb5d1e3dbb343d4e46a19fc2384704aded7a2c5940a208c943ae4fe33; output-bytes=281

- [x] S4: Poison record fails on two actual worker processes, stays uncommitted and blocks its normal successor
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase2.ps1 -Check D
  EXPECT: PHASE2_D_PASS
  EVIDENCE: exit=0; shell=cmd.exe; cwd=project root; path=900acd22c11c/45 entries; EXPECT=matched; output-sha256=e6c749f669704258531105f91bd33afafe42aebbde752d0f6627075e630074b3; output-bytes=282

- [x] R1: Actual per-scenario evidence and regression results are preserved without mixing historical records, resetting offsets or adding later-phase features
  EVIDENCE: SQL/broker CLI checked; latest A-D PASS; 8 PASS and 1 failed observer run retained; historical product 1=81; modules/compose/API/schema/old evidence unchanged; JSON sha256=b7887ec8e87c3e7d0e0c570338c38519a8d1cb617143d8df9aab30587345da61

- [x] R2: Reviewed, secret-free changes are committed locally with the requested message and a clean tree
  EVIDENCE: Requested local commit created; git status --porcelain empty; 14-file allowlist, secret scan and staged diff --check passed; no push. This ledger-only completion record is included by amendment; final hash is git log -1.
