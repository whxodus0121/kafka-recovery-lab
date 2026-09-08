# Gates: Phase 3 fixed retry and DLQ

Scope: Existing stack and inventory flow; no later-phase features. Phase 2 baseline remains reproducible through an explicit legacy failure-mode flag. Historical evidence is preserved.

- [x] B: Formatting, unit tests, vet, build and module verification pass
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase3.ps1 -Check Build
  EXPECT: PHASE3_BUILD_PASS
  EVIDENCE: exit=0; shell=C:\Windows\system32\cmd.exe; cwd=C:\Users\조태연\Desktop\kafka-recovery-lab; path=900acd22c11c/45 entries; EXPECT=matched; output-sha256=0cc7f14127a18d86cf9a2f567ad53ee5a41744710193e880749b4054b74340b9; output-bytes=716

- [x] R: Phase 0/1 and explicit Phase 2 baseline regressions pass
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase3.ps1 -Check Regression
  EXPECT: PHASE3_REGRESSION_PASS
  EVIDENCE: exit=0; shell=C:\Windows\system32\cmd.exe; cwd=C:\Users\조태연\Desktop\kafka-recovery-lab; path=900acd22c11c/45 entries; EXPECT=matched; output-sha256=7310c73c8fd2128a24c29ea583ed22618e6bbc1e3bce00996590f542132cc345; output-bytes=6597

- [x] S: Real normal, poison, recovery, exhaustion, metadata and publication-failure checks pass
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase3.ps1 -Check Scenarios
  EXPECT: PHASE3_SCENARIOS_PASS
  EVIDENCE: exit=0; shell=C:\Windows\system32\cmd.exe; cwd=C:\Users\조태연\Desktop\kafka-recovery-lab; path=900acd22c11c/45 entries; EXPECT=matched; output-sha256=652d5db5f894f7c9085c0c6e03d5d572dd058951524bfecf4b1c12f2188dd960; output-bytes=1371

- [x] T: Retry and DLQ topics are explicitly provisioned with the intended local configuration
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase3.ps1 -Check Topics
  EXPECT: PHASE3_TOPICS_PASS
  EVIDENCE: exit=0; shell=C:\Windows\system32\cmd.exe; cwd=C:\Users\조태연\Desktop\kafka-recovery-lab; path=900acd22c11c/45 entries; EXPECT=matched; output-sha256=fb0fb6880bc5081de305c66d72862f0ca245566197aa53543b9b7555ea33bc1d; output-bytes=412

- [x] D: Evidence, eleven-section technical document and README match measured outcomes and limitations
  EVIDENCE: Latest 8 real scenarios PASS; 16 successful executions and 4 initialization failures retained separately; Main 1 plus Retry 3 failed DB attempts measured; external CLI poison 2/2, recovery 1/1, exhaustion 3/3 and SQL checked; historical evidence unchanged; all document links and 11 sections checked.

- [x] C: Reviewed secret-free changes committed locally with no new dependencies or later-phase features
  EVIDENCE: Requested local commit created after all executable gates passed; 16-file allowlist and secret/staged-diff checks passed; worktree clean verified; modules, compose and historical evidence unchanged; no push. This ledger-only completion record is amended into that commit; final hash is in Git history.
