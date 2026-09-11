# Phase 5 검증 결과

실행일: 2026-09-09 KST. Phase 5 실제 시나리오 7개와 Phase 0~4 기능 회귀가 PASS했다.

## Transaction과 schema

InnoDB `processed_events`는 event_id PRIMARY KEY, payload_hash BINARY(32), order_id, product_id, quantity, processed_at을 가진다. 각 실험에서 `SHOW CREATE TABLE`과 information_schema의 실제 engine을 확인했다. migration은 기존 inventory 초기화 스크립트에서 001 이후 002를 적용하며 Seed를 실행하지 않는다.

정상 경계는 `BEGIN → processed_events INSERT → inventory UPDATE → COMMIT`이다. Duplicate Key 1062이면 locking/current read로 기존 hash와 처리 필드를 확인한다. 동일 payload는 UPDATE 없이 읽기 transaction을 종료하고 offset을 commit한다. 다른 payload는 EVENT_ID_CONFLICT로 기존 DLQ 정책에 연결한다. 기타 DB 오류는 숨기지 않는다.

## Before / After: 같은 crash 경계

| 관측 | Phase 2 baseline | Phase 5 CrashAfter |
| --- | --- | --- |
| 초기 재고 | 100 | 100 |
| 첫 DB commit 후 | 98 | 98, marker 1개 |
| offset commit 전 crash | 미commit | 미commit(-1), exit 87 |
| 동일 record 재전달 후 | 96 | 98, marker 1개·내용 불변 |
| 최종 committed offset | 1 | 1 |

Before는 기존 Phase 2 evidence를 보존하고 `-phase2-baseline`으로 별도 회귀한다. After의 실제 식별자는 다음과 같다.

- eventId: `1998467e-fe2c-49dd-a3e6-5851156f5726`
- orderId: `7fdfc4be-0485-4702-92d0-e1a3d8ad6a8c`
- productId: `178888280715500`, quantity: `2`
- topic: `phase5.1998467e-fe2c-49dd-a3e6-5851156f5726.main`, partition `0`, record offset `0`
- Worker PID: `33680`(exit 87) → `19396`(정상 종료)
- payload hash: `22ab97c9c3cb20e1d268a00d78c844b733624ff899c156ebb06a0c8966700539`

Crash 직후 외부 SQL과 broker 조회는 재고 98, marker 1, source committed -1이었다. 재시작 Worker의 원본 좌표·eventId·raw value hash가 첫 Worker와 같았고, `inventory_duplicate` 1회, 신규 `inventory_committed` 0회였다. 외부 SQL 재고와 marker 전체 내용(처리 시각 포함)이 그대로인 상태에서 source committed가 1로 진행했다. Retry/DLQ 발행은 0건이다.

## 실제 시나리오 결과

| 시나리오 | 판정 | 실측 결과 |
| --- | --- | --- |
| CrashAfter | PASS | 100→98→98, marker 0→1→1, 동일 record 재전달과 offset -1→1 |
| CrashBefore | PASS | COMMIT 전 exit 86: 재고 100·marker 0·offset -1; 재시작 후 재고 98·marker 1·offset 1 |
| Repeated | PASS | Kafka record 100건, handler 100회, 신규 반영 1회, Duplicate 99회, 재고 98·marker 1·offset 100 |
| Concurrent | PASS | 동일 함수 16개 동시 DB 호출, 신규 반영 1회·Duplicate 15회, 재고 98·marker 1 |
| Rollback | PASS | 상품 없음·재고 부족은 marker 0; 실제 SQL 잠금 대기 timeout 후 재고 100·marker 0, 같은 이벤트 정상 재호출 후 98·marker 1 |
| Conflict | PASS | 같은 eventId의 quantity 2→3 변경은 EVENT_ID_CONFLICT DLQ 1건, 재고 98·기존 marker 유지, Main committed 3 |
| RetryDuplicate | PASS | Main 성공 후 같은 eventId를 Retry에 전달: Duplicate 1회, 추가 UPDATE 0회, 재고 98·marker 1·Retry committed 1 |

Repeated의 홀수 record는 필드 순서와 createdAt의 Z/+00:00 표기를 바꿨다. 같은 canonical payload로 판정됐으며 Retry/DLQ는 0건이었다. 이는 100개의 handler 호출과 한 번의 DB side effect를 구분한 실측이다.

Concurrent는 Kafka Consumer를 16개 띄운 실험이 아니다. 실제 transaction 함수를 DB pool 최대 16개 연결로 동시에 호출했다. 첫 transaction을 COMMIT 전 hook에서 잡아둔 동안 외부 SQL은 재고 100·marker 0을 읽었고, information_schema.PROCESSLIST에서 경쟁 INSERT 15개를 확인했다. 해제 후 16개 모두 정상 반환했고 15개는 Duplicate였다. mock이나 Redis lock은 사용하지 않았다.

Rollback의 SQL 실패는 별도 transaction이 inventory row 잠금을 잡고 있는 동안 실제 UPDATE를 800ms context 제한으로 실행한 결과다. `context deadline exceeded` 이후 marker와 재고 변경이 모두 남지 않았고, 잠금을 해제한 뒤 같은 eventId가 신규로 성공했다. 별도 marker 선행 commit이었다면 통과할 수 없는 검사다.

Conflict는 정상 이벤트, 변경 payload, 원래 이벤트를 같은 partition의 offset 0·1·2에 발행했다. 실제 DLQ 원본은 offset 1의 변경 payload와 같았고 오류 코드는 EVENT_ID_CONFLICT였다. 마지막 원래 payload는 정상 Duplicate로 처리돼 source committed는 3이었다. Retry topic에는 발행되지 않았다.

## 검증 근거와 해석

검증 당시 실행 이력은 같은 scenario의 초기 실패를 지우지 않고 마지막 항목을 최종 실행으로 판정했다. event/hash, SQL marker와 재고, broker snapshot, Worker PID/exit/log와 실제 Kafka record를 교차 확인했다. rollback·concurrency는 허용된 DB-level 검증이라 Kafka record를 만들지 않았다.

초기 실행은 topic 생성 관리 요청의 5초 제한에 걸린 2개 FAIL과 나머지 5개 PASS였다. 초기 로그를 보존하고 관리 요청만 30초로 분리했다. 이후 7개가 모두 PASS했다. Worker의 처리 timeout을 늘리거나 비즈니스 오류를 우회하지 않았다. 최초 실패의 정확한 broker 지연 원인까지 검증한 것은 아니다.

Phase 4 회귀 실행에서는 Fixed, Exponential, Full Jitter의 여섯 기능 시나리오가 모두 PASS했다. Retry Header와 `next-attempt-at`, Retry/DLQ 처리 및 전략 계산은 Build/단위 테스트와 실제 시나리오에서 유지됐다. 실측값은 A/fixed 14.115초, A/exponential 10.866초, A/jitter 11.375초, B/fixed 12.300초, B/exponential 13.844초, B/jitter 12.253초였다.

다만 이 중 네 실행은 Phase 4의 과거 성능 비교용 8~12초 장애 길이 조건을 벗어나 audit이 FAIL했다. 이후 재실행에서도 12초 초과가 반복됐고 한 번은 35초 안에 MySQL이 복구되지 않았다. 최근 MySQL 로그에는 연결 종료 시 `unexpected EOF`, 정상 shutdown 중 연결 강제 종료, 느린 재기동이 있었지만 schema/data 오류나 Phase 5 변경이 원인이라는 증거는 없었다. 근본 원인은 `UNVERIFIED`이며 Docker/DB 설정은 바꾸지 않았다. 진단 뒤 Docker Desktop Linux engine도 사용할 수 없는 상태가 되어 추가 반복을 중단했다. 이 조건은 Phase 4에서 보존한 성능 수치의 비교 조건이며 Phase 5 기능 gate가 아니다. 과거 Phase 4 evidence와 기준은 수정하지 않았다.

## 완료 조건별 판정

| # | 조건 | 판정 | 근거 |
| --- | --- | --- | --- |
| 1 | Build/Test | PASS | 포맷·단위 테스트·vet·build·module verify·tag compile/vet |
| 2 | Phase 0~4 기능 회귀 | PASS | Phase 0~3 PASS; Phase 4 전략 계산과 여섯 실제 기능 시나리오 PASS. 과거 성능 비교 재현은 환경 편차로 FAIL이며 별도 기록 |
| 3 | processed_events schema | PASS | 실제 InnoDB/PK/BINARY(32)/DATETIME(6) 조회 |
| 4 | 신규 이벤트 정상 처리 | PASS | SQL 재고 100→98, marker 1, source commit |
| 5 | 동일 transaction | PASS | 첫 transaction을 잡은 상태에서 외부 SQL 재고 100·marker 0; commit 후 둘 다 반영 |
| 6 | 같은 이벤트 DB 변경 없음 | PASS | Duplicate 경로의 재고·marker 전체 내용 불변 |
| 7 | Scenario C After | PASS | 같은 topic/partition/offset/eventId 재전달, 100→98→98 |
| 8 | 반복 처리 side effect 1회 | PASS | 100회 Kafka 전달, 신규 반영 1·Duplicate 99 |
| 9 | 동시 duplicate | PASS | 실제 DB 함수 16개 호출, 신규 반영 1·Duplicate 15 |
| 10 | 실패 transaction rollback | PASS | 상품 없음·재고 부족·SQL timeout·commit 전 crash의 marker 0 |
| 11 | payload conflict | PASS | 변경 payload DLQ EVENT_ID_CONFLICT, 기존 재고·marker 보존 |
| 12 | Retry topic duplicate | PASS | 재고 98·marker 1, 신규 반영 0·Duplicate 1 |
| 13 | 실제 Kafka offset | PASS | broker end/committed와 원본 record 직접 조회 |
| 14 | 검증 근거와 보고서 | PASS | 실패와 외부 SQL/broker/프로세스 증거를 함께 대조 |
| 15 | 상세 문서/README | PASS | 같은 11개 섹션과 Development Journey 연결 |
| 16 | Phase 6 없음 | PASS | Replay/Recovery/Rate Limit 및 HOL 개선 미구현 |

## 실행 명령

실제로 각 Check를 개별 호출했다. Scenarios는 초기화 보완 후 다시 실행했다.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase5.ps1 -Check Build
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase5.ps1 -Check Scenarios
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase5.ps1 -Check Regression
```

Build는 `gofmt`, `go test ./...`, `go vet ./...`, `go build ./...`, `go mod verify`와 Phase 5 integration tag compile/vet을 포함한다. Scenarios는 기존 migration 스크립트를 Seed 없이 실행한다. 당시 Regression 실행은 Phase 0~3과 Phase 4 실제 6개 장애 시나리오를 실행했다. 기능 시나리오는 모두 통과했지만 과거 성능 비교 조건 audit은 위 환경 편차로 실패했다. 수정된 Phase 5 Regression 진입점은 Phase 4 Build와 Phase 0~3 회귀, 보존 evidence 정합성만 확인하며 6개 성능 실험을 반복하지 않는다. 과거 Phase 4 raw/evidence는 수정하지 않는다.

## 범위와 남은 한계

추가 외부 dependency는 없다. `go.mod`, `go.sum`, Compose와 기존 inventory schema는 그대로다. 기존 kafka-go, database/sql과 MySQL driver를 사용한다. 002 migration, transaction 등록/비교, 명시적 Duplicate 결과, Consumer 로그/분기, 최소 테스트·검증·문서를 추가했다.

Phase 6 이상의 Replay/Recovery/Rate Limit, Redis/분산 lock, Kafka transaction/Outbox, scheduler/HOL 개선은 구현하지 않았다. Idempotency는 Retry/DLQ를 대체하지 않는다.

운영 규모 성능, Kafka 다중 Worker rebalance, marker 보존·삭제 정책, 과거 처리 backfill은 UNVERIFIED 또는 미구현 범위다. 이미 처리했지만 marker가 없는 과거 이벤트, 서로 다른 eventId의 같은 주문, marker 삭제 이후 재전달까지 보호한다고 주장하지 않는다.
