# Gates: Phase 0 local environment

OWNS: compose.yaml, .env.example, .gitignore, go.mod, go.sum, cmd/smoke/**, internal/config/**, scripts/**, README.md, docs/phase-0-verification.md, GATES.md

Scope: Go, single-node KRaft Kafka and persistent MySQL only. No Phase 1 business functionality. Commands below implement the execution checks explicitly requested by the user.

- [x] G1: Go packages build and pass configuration tests, vet and module verification
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check Build
  EXPECT: BUILD_PASS
  EVIDENCE: exit=0; shell=C:\Windows\system32\cmd.exe; cwd=C:\Users\조태연\Desktop\kafka-recovery-lab; path=8e091ab82072/45 entries; EXPECT=matched; output-sha256=bdd28a870f4c840533ad58bd3df67321146112880d860450234cc3ff34126fa1; output-bytes=130

- [x] G2: Compose starts exactly Kafka and MySQL and both authenticated/protocol healthchecks pass
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check Infrastructure
  EXPECT: INFRASTRUCTURE_PASS
  EVIDENCE: exit=0; shell=C:\Windows\system32\cmd.exe; cwd=C:\Users\조태연\Desktop\kafka-recovery-lab; path=8e091ab82072/45 entries; EXPECT=matched; output-sha256=06d7d995c724e6e0f80df5e2abaf8a80c22df4b5680d25816e51758ee04e531a; output-bytes=624

- [x] G3: Explicit topic initialization is repeatable and verifies the orders and smoke topic configuration
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check Topics
  EXPECT: TOPICS_PASS
  EVIDENCE: exit=0; shell=C:\Windows\system32\cmd.exe; cwd=C:\Users\조태연\Desktop\kafka-recovery-lab; path=8e091ab82072/45 entries; EXPECT=matched; output-sha256=6804af2eec3b12030427d1613cd5c2b1d67ddc7ae09fdd0b83da57a26b2fa097; output-bytes=1637

- [x] G4: kafka-go produces and consumes the exact unique smoke message
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check Kafka
  EXPECT: KAFKA_PASS
  EVIDENCE: exit=0; shell=C:\Windows\system32\cmd.exe; cwd=C:\Users\조태연\Desktop\kafka-recovery-lab; path=8e091ab82072/45 entries; EXPECT=matched; output-sha256=b2f021c20825d90e844540f08f9d77a02f357f86f6c35dbe01c723391e4b1994; output-bytes=202

- [x] G5: database/sql authenticates and verifies SELECT result, MySQL version and InnoDB
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check MySQL
  EXPECT: MYSQL_PASS
  EVIDENCE: exit=0; shell=C:\Windows\system32\cmd.exe; cwd=C:\Users\조태연\Desktop\kafka-recovery-lab; path=8e091ab82072/45 entries; EXPECT=matched; output-sha256=909fd21bfbcf7993afde65b3426d38d030aaeca9f2528a546822c36803c64365; output-bytes=56

- [x] G6: A unique InnoDB probe row survives a real MySQL container restart and is cleaned up
  CHECK: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check Persistence
  EXPECT: PERSISTENCE_PASS
  EVIDENCE: exit=0; shell=C:\Windows\system32\cmd.exe; cwd=C:\Users\조태연\Desktop\kafka-recovery-lab; path=8e091ab82072/45 entries; EXPECT=matched; output-sha256=e49eaab88ef69f68f57eba1abe3d94d3c0f22eb5899fb20973f99df52687e588; output-bytes=526

- [x] G7: Source and direct/transitive dependencies stay within Phase 0 scope
  EVIDENCE: 2026-09-07 manual source review plus live queries: only cmd/smoke and internal/config Go packages; 2 direct and 3 runtime indirect modules; Compose services kafka,mysql; MySQL SHOW TABLES returned only phase0_probe with 0 rows; orders.created.v1 has 6 partitions each at end offset 0; no business API, worker, tables or recovery features.

- [x] G8: Git state, secret exclusion, reproducible commands, pinned versions and verification limitations are documented
  EVIDENCE: 2026-09-07 manual README/report review against the request; git lists 15 untracked project files on main with no commits or remotes; .env ignored; exact local-password comparison across all repository-visible files returned SECRET_EXCLUSION_PASS without exposing values; Go/module versions and live image digests recorded; initial failures and final resolution recorded; G1-G6 have successful executable evidence.
