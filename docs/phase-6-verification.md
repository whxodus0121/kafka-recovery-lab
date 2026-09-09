# Phase 6 검증 결과

실행일: 2026-09-10 KST. 필수 Phase 6 항목은 PASS 15 / FAIL 0 / UNVERIFIED 0이다. 범위 밖 운영 조건은 별도로 `UNVERIFIED`로 남겼다.

## 실제 복구 결과

Retry Exhaustion은 MySQL 컨테이너를 중지하지 않고 해당 Worker 프로세스의 DB port만 닫힌 port로 바꿔 결정적으로 만들었다. 추가 Retry 2회를 소진한 뒤 실제 DLQ record `phase6.a41feabc-66d6-4543-b8db-ad1b9f6f72f7.dlq/0/0`을 선택했다.

eventId `7e822375-c9df-49d0-832b-0fdb7e5d8b5c`, productId `1788968430106821200`, quantity 2의 상태 변화는 다음과 같다.

| 경계 | Inventory | processed_events | Recovery end | Recovery committed |
| --- | ---: | ---: | ---: | ---: |
| Retry 소진 후 | 100 | 0 | 0 | -1 |
| 첫 Replay CLI 성공 직후 | 100 | 0 | 1 | -1 |
| Recovery Worker 처리 후 | 98 | 1 | 1 | 1 |
| 같은 DLQ 두 번째 Replay 처리 후 | 98 | 1 | 2 | 2 |

첫 Replay ID는 `8cf03ff8-4082-4559-b911-fe606c1f4510`, 두 번째는 `8f0e27ac-718a-4919-a817-ae971e16f58f`다. 같은 DLQ record를 각각 선택한 독립 실행이므로 두 record의 `replay-count`는 모두 1이다. 두 번째 처리는 `inventory_duplicate`였으며 marker 내용과 재고는 변하지 않았다. Worker PID는 Main `19828`, Retry `27196`, Recovery `3844`다.

## 실패 경로와 metadata

Recovery 처리 중 닫힌 DB port를 사용한 실제 연결 실패는 `inventory.retry`로 발행됐고 `retry-count=1`부터 시작했다. replay header와 원래 Main 좌표가 유지됐으며 정상 Retry Worker 처리 후 inventory 100→98, marker 0→1, Recovery/Retry committed offset은 각각 1이었다.

상품이 없는 eventId `8d2c6fe6-ba54-4321-8bca-9aedf5b0e684`는 Recovery에서 `PRODUCT_MISSING`으로 다시 DLQ에 들어갔다. 첫 실패 DLQ는 offset 1, `replay-count=1`; 그 record를 다시 Replay한 뒤 생긴 DLQ는 offset 2, `replay-count=2`였다. 최종 Recovery committed offset은 2이고 inventory row와 processed marker는 모두 0이다. 이것은 복구 성공이 아니라 실패 lineage 보존 결과다.

malformed DLQ envelope와 존재하지 않는 Recovery Topic 발행은 CLI exit failure였다. DLQ end offset은 2로 유지됐고 관측용 DLQ group committed offset은 -1, 정상 Recovery Topic end offset은 0이었다. CLI는 DLQ record를 삭제하거나 group offset을 사용하지 않았다.

## 완료 조건

| # | 조건 | 판정 | 실제 근거 |
| --- | --- | --- | --- |
| 1 | Build/Test | PASS | 전체 단위 테스트, vet, build, module verify, Phase 6 tag compile/vet |
| 2 | 단일 DLQ 좌표 선택 | PASS | topic/partition/offset 1건 직접 ReadMessage |
| 3 | Exhaustion→Replay→DB 복구 | PASS | 100·marker 0에서 98·marker 1 |
| 4 | publication/복구 구분 | PASS | 발행 직후 DB 불변·committed -1, Worker 후 commit 1 |
| 5 | 동일 DLQ 2회 Replay | PASS | 100→98→98, marker 0→1→1 |
| 6 | Recovery Idempotency | PASS | 기존 Store 사용, 신규 1회·Duplicate 1회 |
| 7 | Recovery Retryable | PASS | 새 `retry-count=1`, 최종 Retry 처리 성공 |
| 8 | Recovery Domain→DLQ | PASS | PRODUCT_MISSING, source commit, DB row/marker 0 |
| 9 | replay metadata | PASS | replay ID, count 1→2, DLQ 좌표와 원래 좌표 보존 |
| 10 | malformed envelope | PASS | CLI 실패, Recovery 발행 0 |
| 11 | Recovery publish 실패 | PASS | 없는 Topic 동기 발행 실패, source 불변 |
| 12 | Kafka offsets | PASS | end/committed와 실제 record를 Broker에서 조회 |
| 13 | MySQL 상태 | PASS | inventory와 processed_events를 외부 SQL로 조회 |
| 14 | Phase 0~5 영향 회귀 | PASS | Build chain, Retry/idempotency unit, Phase 4~5 evidence 정합성 |
| 15 | Phase 7 기능 없음 | PASS | bulk/rate limit/checkpoint/scheduler 미구현 |

## 실행 명령

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase6.ps1 -Check Build
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase6.ps1 -Check Topics
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase6.ps1 -Check Scenarios
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase6.ps1 -Check Regression
```

Phase 4의 여섯 성능 실험은 실행하지 않았다. Phase 6가 delay 계산을 바꾸지 않았으므로 계산 단위 테스트와 보존 evidence만 확인했다. 최초 Regression은 Phase 4 evidence의 실제 키 `Status`를 소문자로 찾은 검사 코드 때문에 FAIL했고, 원본을 바꾸지 않고 검사만 수정한 뒤 PASS했다.

## Evidence와 한계

[phase-6-evidence.json](phase-6-evidence.json)은 각 실행의 DLQ/Recovery/Retry record, header, end/committed offset, SQL 상태, PID와 시각을 보존한다. 실패 실행을 성공으로 바꾸거나 과거 Phase evidence를 수정하지 않았다.

추가 외부 dependency는 없다. 기존 `github.com/segmentio/kafka-go`, `database/sql`, MySQL driver만 사용하며 `go.mod`, `go.sum`, Compose와 DB schema는 변경하지 않았다.

Bulk Replay, Rate Limiting, replay checkpoint, pause/resume, 장기 보존 정책, 여러 partition과 rebalance, 운영 규모 성능은 이번 범위에서 실행하지 않아 `UNVERIFIED`다. Replay 발행 성공과 CLI 성공 응답 사이 crash로 같은 Recovery record가 더 생길 수 있으며, DB side effect는 Idempotency로 보호되지만 Kafka record 중복 자체가 제거되는 것은 아니다.
