# Gates: Phase 4 retry timing comparison

Scope: Preserve Phase 3 fixed default and Phase 2 duplicate effects. Add exponential/full jitter timing only; isolated real MySQL outage experiments, no scheduler or Phase 5 features. Historical evidence is not overwritten.

- [x] B: Build, unit timing contracts, vet and module verification pass
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase4.ps1 -Check Build
  EXPECT: PHASE4_BUILD_PASS
  EVIDENCE: exit=0; shell=PowerShell; cwd=project root; EXPECT matched; final code build output bin/phase4-build-run.log sha256=b5f9c6b7a4367c578d4062fe25073d1881efe92f31fc4f4e8d9066dd768c2007; all formatting, unit, vet, build, module and tagged compile checks passed.

- [x] R: Phase 0 through 3 regressions pass with separate evidence
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase4.ps1 -Check Regression
  EXPECT: PHASE4_REGRESSION_PASS
  EVIDENCE: exit=0; shell=PowerShell; cwd=project root; EXPECT matched; bin/phase4-regression-run.log sha256=0a53816b0bf6da056232ff506179134996f5d25a53364e2ba0798ae2f35fe2bd; bytes=8438; Phase 0/1, Phase 2 A-D and Phase 3 eight scenarios passed; first initialization failure preserved in regression evidence.

- [x] S: Six isolated real outage runs preserve raw records, timing, lag and reconciled outcomes
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase4.ps1 -Check Scenarios
  EXPECT: PHASE4_SCENARIOS_PASS
  EVIDENCE: exit=0; shell=C:\Windows\system32\cmd.exe; cwd=project root; EXPECT=matched; output-sha256=34144d3f8a958d48d498ca04e89103c8134f18e9b5e5685ad78f133ada66fda8; output-bytes=6953; A/fixed repeated in fresh topics after startup outlier, selected test exit=0 (dfb0a1), actual outage=9.883s; 7 immutable raw files retained, qualification decided by gate A.

- [x] A: Offline evidence audit verifies headers, timing, counts, identical planned controls and HOL metrics
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase4.ps1 -Check Audit
  EXPECT: PHASE4_AUDIT_PASS
  EVIDENCE: exit=0; shell=C:\Windows\system32\cmd.exe; cwd=C:\Users\조태연\Desktop\kafka-recovery-lab; path=900acd22c11c/45 entries; EXPECT=matched; output-sha256=b64dfb9a75c530aba49368c48da585fc2a5b735342bc1053de73cd306d75bff1; output-bytes=1483

- [x] D: Eleven-section document, verification and README accurately interpret measured data and actual outage variation
  EVIDENCE: All three measured tables matched evidence fields; 15 required rows and 11 detail sections checked; local links and all 7 raw SHA-256 values checked; 6 qualified cells plus 1 startup outlier retained; common 8s comparison, actual outage variation, HOL, initial regression/audit failures and unverified extensions explicitly documented.

- [x] C: Reviewed secret-free changes committed locally without new dependencies or later-phase functionality
  EVIDENCE: Requested local commit created after executable and documentation gates passed; 23-file allowlist, actual password scan, staged diff check and raw Git blob byte comparison passed; clean worktree verified; no dependency, Compose, schema or Phase 5 additions; no push. This ledger-only completion record is amended into the same local commit; final hash is in Git history.
