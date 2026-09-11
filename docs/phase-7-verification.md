# Phase 7 검증 결과

실행일: 2026-09-10 KST. 필수 완료 조건은 PASS 19 / FAIL 0 / UNVERIFIED 0이다. 범위 밖 운영 조건은 별도 `UNVERIFIED`로 남겼다.

## 공통 조건과 전략 결과

기존 DLQ envelope 계약으로 만든 유효 fixture 120건을 실제 Kafka DLQ Topic에 저장했다. 12개 product, 각 초기 재고 1,000, quantity 1을 round-robin으로 사용했다. 전략마다 새 eventId, topic, group을 사용했고 offset reset은 하지 않았다.

| 전략 | publish-rate | recovery-rate | 발행 duration / RPS / peak | 처리 duration / RPS / peak | peak lag | CLI / business 완료 | 성공 / DLQ / 미완료 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Unlimited | 0 | 0 | 1,641 ms / 72.48 / 81 | 1,616 ms / 73.63 / 81 | 5 | 2,498 / 2,498 ms | 120 / 0 / 0 |
| Publication Limited | 20 | 0 | 6,065 ms / 19.62 / 20 | 6,063 ms / 19.62 / 23 | 3 | 6,842 / 6,842 ms | 120 / 0 / 0 |
| Backlog + Recovery Limited | 0 | 20 | 1,634 ms / 72.80 / 81 | 6,073 ms / 19.59 / 20 | 120 | 2,196 / 8,355 ms | 120 / 0 / 0 |

모든 전략은 inventory 12,000→11,880, processed_events 120건을 확인했고 `success + DLQ + unfinished = 120`이었다. Publication Limited에서 processing peak가 23으로 발행 peak 20보다 컸다. Producer pacing은 실제 DB 처리 peak의 동일 상한이 아니다.

## Idempotency·단건·부분 실패

Bulk 24건의 첫 Replay는 신규 반영 24건이었다. 같은 DLQ 범위의 두 번째 Replay는 Duplicate 24건이며 추가 inventory 변경 0, processed_events 24 유지, Recovery committed 24→48이었다.

기존 `-partition -offset` 단건 경로는 1건을 실제 DB에 반영하고 Recovery end/committed 1을 확인했다. Bulk 부분 실패는 유효 offset 0을 발행한 뒤 malformed offset 1에서 CLI가 실패했다. 이미 발행된 수 1과 실패 좌표를 보고했고 Recovery end 1, DLQ end 3, DLQ committed -1이었다.

## 완료 조건

| # | 조건 | 판정 | 실제 근거 |
| --- | --- | --- | --- |
| 1 | Build/Test | PASS | 전체 단위 test, module verify, build, tag compile/vet |
| 2 | 기존 단건 Replay | PASS | 단건 1건 DB 반영, Recovery committed 1 |
| 3 | Bulk DLQ selection | PASS | partition 0, start 0부터 120건 순차 좌표 확인 |
| 4 | Bulk Recovery publication | PASS | Recovery end 120, 각 record metadata 보존 |
| 5 | publication limiter | PASS | 20/s 설정, 6,065 ms, 평균 19.62, peak 20 |
| 6 | recovery processing limiter | PASS | backlog 120에서 평균 19.59, peak 20 |
| 7 | Unlimited 실험 | PASS | 실제 Kafka/MySQL 120건 완료 |
| 8 | Publication Limited 실험 | PASS | 실제 timestamp 120개 측정 |
| 9 | Backlog + Recovery Limited | PASS | 시작 lag 120, 최종 committed 120 |
| 10 | publication RPS | PASS | timestamp·elapsed·sliding peak 교차 계산 |
| 11 | processing RPS | PASS | Store 전 시작 timestamp 120개 계산 |
| 12 | lag | PASS | broker end/committed 반복 조회, peak 5/3/120 |
| 13 | reconciliation | PASS | 세 전략 모두 120+0+0=120 |
| 14 | DB 결과 | PASS | inventory 11,880, marker 120 |
| 15 | Bulk duplicate Idempotency | PASS | 신규 24, Duplicate 24, 추가 차감 0 |
| 16 | CLI/business 완료 분리 | PASS | backlog 실행 2,196 ms와 8,355 ms |
| 17 | Phase 5/6 핵심 회귀 | PASS | 기존 검증 결과, 단건, Store/idempotency unit |
| 18 | 문서/evidence | PASS | raw 6개, evidence, 검증·상세 문서·gates |
| 19 | Phase 8 기능 없음 | PASS | checkpoint/scheduler/분산 limiter 미구현 |

## 실행 명령

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase7.ps1 -Check Build
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase7.ps1 -Check Topics
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase7.ps1 -Check Scenarios
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase7.ps1 -Check Regression
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase7.ps1 -Check Audit
```

모든 명령 exit 0이었다. Phase 4의 실제 성능 실험과 과거 raw evidence는 재실행하거나 수정하지 않았다.

## 검증 근거와 한계

선택한 PASS 실행의 실제 publication/processing timestamp, 초별 bin, 평균·peak, end/committed offset, inventory, marker와 PID를 교차 확인했다. 실패 실행을 성공 결과로 바꾸지 않았다.

Rate peak는 OS scheduling 영향을 받는 1초 sliding-window 관측값이다. Recovery rate는 최초 Recovery business 처리에만 적용되며 이후 Retry Worker를 통제하지 않는다. 여러 partition, checkpoint/resume, crash 자동 복구, 반복 실험 통계, 운영 규모는 `UNVERIFIED`다.

추가 외부 dependency는 없다. `go.mod`, `go.sum`, Compose와 DB schema를 변경하지 않았다.
