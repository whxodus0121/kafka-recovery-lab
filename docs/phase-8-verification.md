# Phase 8 검증 결과

실행일: 2026-09-10 KST. 필수 완료 조건은 **PASS 26 / FAIL 0 / UNVERIFIED 0**이다. 선택 항목인 정확한 committed lag metric은 구현하지 않아 별도 `UNVERIFIED`로 기록했다.

## 실제 metric 변화

| 시나리오 | Prometheus query 결과 | Kafka/MySQL 교차 검증 |
| --- | ---: | --- |
| Normal | success 0→1, processing count 0→1 | inventory 100→98, main committed 1 |
| Duplicate | duplicate 0→1 | inventory 98 유지, marker 1 |
| DLQ | MALFORMED_JSON 0→1 | main committed 3, DLQ end 증가 |
| Retry | DB_CONNECTION 0→1 | failed Main/Retry committed 1, inventory 100→97 |
| Retry overdue | fixed count 0→1 | Retry Worker 성공 0→1 |
| 단건 Recovery | recovery success 0→1 | inventory 100→99, committed 1 |
| Recovery 5/s | wait count 0→9 | 10건 성공, 최종 Recovery committed 11 |

Prometheus target은 `host.docker.internal:22112`, `:22113`, `:22114` 모두 UP이었다. 실제 label은 `error_code`, `le`, `outcome`, `strategy`, `worker`만 관측됐다. Grafana datasource와 9개 panel이 provision됐고 10개 panel query가 모두 성공했다.

## 완료 조건

| # | 조건 | 판정 | 근거 |
| --- | --- | --- | --- |
| 1 | Build/Test | PASS | gofmt, test, vet, build, mod verify, Phase 8 tag compile/vet |
| 2 | Prometheus client | PASS | client_golang v1.23.2 직접 의존성 |
| 3 | Worker metrics endpoint | PASS | 세 process `/metrics`, 독립 주소 |
| 4 | processing counter | PASS | Normal 0→1 |
| 5 | processing histogram | PASS | Store 성공 count 0→1 |
| 6 | Retry metric | PASS | DB_CONNECTION 0→1 |
| 7 | DLQ metric | PASS | MALFORMED_JSON 0→1 |
| 8 | Duplicate metric | PASS | 실제 Duplicate 0→1, 재고 불변 |
| 9 | Retry overdue metric | PASS | fixed count 0→1, 음수 clamp |
| 10 | Recovery metric | PASS | 단건 success 0→1 |
| 11 | Recovery limiter wait | PASS | 10건/5s 설정에서 양수 wait 9회 |
| 12 | Prometheus target UP | PASS | main/retry/recovery 3개 target API 확인 |
| 13 | Prometheus 전후 query | PASS | 9개 scenario observation |
| 14 | Normal | PASS | DB와 committed offset 일치 |
| 15 | Retry | PASS | 실제 connection refusal→Retry→DB 성공 |
| 16 | DLQ | PASS | malformed record DLQ publication |
| 17 | Duplicate | PASS | inventory 98, marker 1 유지 |
| 18 | Recovery | PASS | Phase 6 단건 경로 실제 DB 복구 |
| 19 | small rate-limit | PASS | 10건 전부 성공, wait histogram 증가 |
| 20 | Grafana datasource | PASS | provisioned UID와 URL API 확인 |
| 21 | Grafana dashboard | PASS | UID와 9개 panel API 확인 |
| 22 | panel query | PASS | datasource proxy 10개 query success |
| 23 | cardinality audit | PASS | 금지 식별자/동적 topic label 없음 |
| 24 | 기존 business 회귀 | PASS | Retry/Idempotency/Replay/Rate unit와 Phase 4~7 evidence |
| 25 | 문서/evidence | PASS | raw, hash, verification, 11-section 문서, gates |
| 26 | Phase 9 기능 없음 | PASS | alert/tracing/checkpoint 미구현 |

## 실행 명령

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase8.ps1 -Check Build
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase8.ps1 -Check Infrastructure
go test '-tags=integration,phase2,phase3,phase4,phase5,phase6,phase7,phase8' -count=1 -timeout=6m -run '^TestPhase8$' -v ./tests/integration
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase8.ps1 -Check Regression
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase8.ps1 -Check Audit
```

Phase 4와 Phase 7 성능 실험은 재실행하지 않았고 과거 raw evidence도 재생성하지 않았다.

## 실패 실행과 해결

PASS 이전 네 raw 실행은 그대로 보존했다.

- Windows 예약 port 2112~2114 bind 실패 → 22112~22114 사용
- 단건 Replay에 빈 `-limit` 전달 → single 인자 구성 수정
- malformed DLQ offset 0을 Recovery 대상으로 선택 → 실제 DLQ end에서 선택
- 미발생 CounterVec descriptor 누락 → bounded label series만 0 등록

초기 Build 회귀는 Phase 1 checker의 dependency 무변경 조건이 허용된 Phase 8 의존성까지 거부했다. Phase 8 checker에서 전체 Go 검증을 직접 실행해 PASS했고 과거 checker는 바꾸지 않았다.

## Evidence와 한계

[phase-8-evidence.json](phase-8-evidence.json)은 PASS raw 경로와 SHA-256, endpoint, target health, query timestamp와 전후 값, DB/Kafka 상태, PID, Grafana provisioning, cardinality 결과를 보존한다. [experiments/phase8](../experiments/phase8/)에는 실패 4개와 PASS 1개가 각각 남아 있다.

정확한 committed consumer-group lag, 운영 network/TLS/auth, 장기 metric 보존, 반복 부하 통계는 `UNVERIFIED`다. Replay CLI는 short-lived라 scrape하지 않았다.

추가 직접 dependency는 `github.com/prometheus/client_golang v1.23.2` 하나다. promhttp 전이 의존성 때문에 `github.com/klauspost/compress`는 v1.15.9에서 v1.18.0으로 올라갔다. Compose에는 digest를 고정한 Prometheus v3.5.0과 Grafana 12.1.1만 추가했다.
