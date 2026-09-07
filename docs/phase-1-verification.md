# Phase 1 실행 보고

정상 흐름 검증 시각: 2026-09-07 10:24:29 KST (증거 JSON의 UTC 01:24:29). 루트: `C:\Users\조태연\Desktop\kafka-recovery-lab`.

## 1. Phase 0 commit 결과

- 성공: `856f76148bc864084c1e6b57e5ccd5eadf89fdd3`.
- 메시지: `chore: set up phase 0 development environment`.
- 정확한 파일 15개 allowlist와 실제 로컬 비밀번호 제외 검사 후 commit했다.
- commit 전 세 파일의 EOF 빈 줄을 정리해 `git diff --cached --check`를 통과했다. 이는 commit 실행 전 공백 수정이며 commit 실패는 없었다.
- 직후 working tree는 clean, push는 하지 않았다.

## 2. Phase 1 결과 요약

`POST /orders → OrderCreated → orders.created.v1 → inventory-main-v1 → MySQL inventory → offset commit`을 실제로 구현·실행했다.

사용자 완료 조건 **PASS 8 / FAIL 0 / UNVERIFIED 0**. 정상 요청 7건의 quantity 합계 19를 재고 100에서 차감하여 81을 확인했다. 잘못된 요청 5건은 발행하지 않았다. 실제 Worker PID를 바꿔 정상 재시작한 후 추가 처리 0건을 확인했다.

이는 정상 흐름의 판정이다. DB 장애·처리 도중 crash·강제 rebalance와 멱등성은 검증한 것이 아니다.

## 3. 생성·수정 파일

수정 5개: `.env.example`, `README.md`, `cmd/smoke/mysql.go`, `internal/config/config.go`, `scripts/env.ps1`.

신규 19개:

```text
PHASE1_GATES.md
cmd/api/main.go
cmd/worker/main.go
internal/event/order_created.go
internal/event/order_created_test.go
internal/inventory/consumer.go
internal/inventory/consumer_test.go
internal/inventory/store.go
internal/kafka/client.go
internal/mysql/mysql.go
internal/order/http.go
internal/order/http_test.go
migrations/001_inventory.sql
scripts/init-inventory.ps1
scripts/seed-inventory.sql
scripts/verify-phase1.ps1
tests/integration/phase1_test.go
docs/phase-1-evidence.json
docs/phase-1-verification.md
```

`go.mod`, `go.sum`, `compose.yaml`, Phase 0 증거 장부·보고서는 변경하지 않았다. 로컬 검증 중 `bin/worker.exe`를 만들었으며 기존 `.gitignore`의 `/bin/`으로 제외된다. 통합 테스트의 나머지 실행 파일은 임시 디렉터리에서 생성·정리한다.

## 4. 파일별 역할

| 파일 | 역할 |
| --- | --- |
| `.env.example` | 선택적 API_ADDR와 KAFKA_CONSUMER_GROUP 예시 |
| `README.md` | 현재 Phase 1 실행·검증·제약 설명 |
| `cmd/smoke/mysql.go` | 기존 smoke를 공유 DB 연결 함수로 연결 |
| `internal/config/config.go` | 기존 설정을 유지하며 API 주소·group 기본값 제공 |
| `scripts/env.ps1` | 새 선택 변수 허용, 기존 Phase 0 .env 호환 |
| `PHASE1_GATES.md` | 실행·소스·문서·커밋 확인 장부 |
| `cmd/api/main.go` | net/http 서버, Producer 수명과 정상 종료 |
| `cmd/worker/main.go` | 별도 Consumer 프로세스, DB·Reader 수명과 정상 종료 |
| `internal/event/order_created.go` | 이벤트 구조, crypto/rand UUID v4, UTC와 필수 값 검증 |
| `internal/event/order_created_test.go` | 계약 위반 이벤트 거절 검증 |
| `internal/inventory/consumer.go` | 순차 fetch → DB → 명시적 Kafka commit |
| `internal/inventory/consumer_test.go` | DB/validation 실패 시 commit·다음 fetch 미호출 검증 |
| `internal/inventory/store.go` | 조건부 원자 UPDATE와 DB transaction commit |
| `internal/kafka/client.go` | 기존 kafka-go의 Producer/Reader 설정 |
| `internal/mysql/mysql.go` | database/sql 연결과 기존 MySQL driver 공유 |
| `internal/order/http.go` | 요청 제한·검증·이벤트 발행 확인 후 응답 |
| `internal/order/http_test.go` | 잘못된 입력의 미발행, 발행 성공 전 응답 금지, 실패 응답 검증 |
| `migrations/001_inventory.sql` | InnoDB inventory 테이블·CHECK 제약 |
| `scripts/init-inventory.ps1` | schema 적용과 명시적 선택 seed |
| `scripts/seed-inventory.sql` | 로컬 product 1 재고 100 초기화 |
| `scripts/verify-phase1.ps1` | Build/Phase0/Flow 검증 진입점 |
| `tests/integration/phase1_test.go` | 실제 자식 프로세스와 Kafka/DB/HTTP 독립 관측 |
| `docs/phase-1-evidence.json` | 실제 이벤트, DB 수량, group offset, PID, 프로세스 로그 |
| `docs/phase-1-verification.md` | 이 보고서 |

Reader 인터페이스와 HTTP의 publish 함수는 테스트 경계에만 사용한다. 범용 Repository/Factory/Event Bus 계층은 없다.

## 5. 최종 디렉터리 구조

```text
.
├── .env.example, .gitignore, compose.yaml, go.mod, go.sum
├── README.md, GATES.md, PHASE1_GATES.md
├── cmd/
│   ├── api/main.go
│   ├── worker/main.go
│   └── smoke/{main.go,kafka.go,mysql.go}
├── internal/
│   ├── config/{config.go,config_test.go}
│   ├── event/{order_created.go,order_created_test.go}
│   ├── inventory/{consumer.go,consumer_test.go,store.go}
│   ├── kafka/client.go
│   ├── mysql/mysql.go
│   └── order/{http.go,http_test.go}
├── migrations/001_inventory.sql
├── scripts/
│   ├── env.ps1, verify.ps1
│   ├── init-inventory.ps1, seed-inventory.sql
│   └── verify-phase1.ps1
├── tests/integration/phase1_test.go
└── docs/
    ├── phase-0-verification.md
    ├── phase-1-verification.md
    └── phase-1-evidence.json
```

`.git/`, `.env`, `bin/`는 로컬 관리·비공개·빌드 산출물이다. 선택적 게이트 도구의 `.unlazy/`도 ignored이며 실행 의존성이 아니다.

## 6. 추가 외부 의존성

**없음.** Phase 0의 Go 1.26.5, kafka-go v0.4.51, go-sql-driver/mysql v1.10.0 및 전이 의존성을 그대로 쓴다. `go.mod`와 `go.sum` diff는 없다. UUID는 crypto/rand, HTTP는 net/http, 로그는 log/slog, 테스트는 testing을 사용한다. 인프라도 기존 Kafka/MySQL 두 서비스다.

## 7. Order API 계약

- `POST /orders`, Content-Type `application/json`.
- body: 양의 int64 `productId`, `quantity`. 최대 4096 bytes.
- 누락·0/음수·비정수·알 수 없는 필드·여러 JSON 값은 400. 미지원 Content-Type은 415, method는 405.
- Kafka의 동기 발행 성공 확인 후 202와 `status=accepted`, `eventId`, `orderId`를 반환한다.
- 발행 확인 실패는 503. timeout 등에서는 broker에 저장됐을 가능성이 있으므로 결과 불확실성을 응답에 명시한다.
- 재고는 비동기로 처리한다. 상품 유무/재고 부족을 API에서 미리 조회하지 않는다.

## 8. OrderCreated 계약

`schemaVersion=1`, UUID v4 `eventId`·`orderId`, 양의 int64 `productId`·`quantity`, UTC `createdAt`. 같은 계약으로 Producer와 Consumer에서 검증한다. 실제 전체 이벤트는 증거 JSON의 records 배열에 있다. Kafka key와 event.orderId가 다르면 Consumer는 commit 없이 실패한다. 오류를 Retryable/Non-Retryable로 분류하는 시스템은 없다.

## 9. Kafka Producer

Topic `orders.created.v1`, key `orderId`, Hash balancer, `RequiredAcks=RequireAll`, `Async=false`, `AllowAutoTopicCreation=false`, `MaxAttempts=1`. BatchTimeout 10ms, read/write timeout 10초, HTTP publish context 10초다. Kafka Transaction은 없다.

단일 broker/RF=1이므로 acks=all은 다중 복제 내구성을 보장하지 않는다. `MaxAttempts`는 이 Writer의 전송 설정이며 업무 메시지 Retry를 구현한 것이 아니다.

## 10. Consumer Group / offset commit

기본 group은 `inventory-main-v1`. `FetchMessage`는 자동 commit하지 않으며, `CommitInterval=0`으로 명시적 `CommitMessages`를 동기로 수행한다. Kafka에 저장되는 값은 처리한 record offset의 다음 값이다.

전 파티션을 합쳐 한 번에 한 메시지를 처리한다. DB 성공 전 또는 DB 실패 후 commit하지 않는다. DB/validation/commit 오류는 즉시 반환하여 뒤 메시지까지 진행하지 않는다. 한 실패가 전체 Worker 진행을 중지하는 대가가 있다.

종료는 context 취소와 Reader.Close를 사용한다. 실제 테스트에서는 부모 프로세스가 `-shutdown-on-stdin-close` 옵션의 stdin을 닫아 정상 종료한다. 새 프로세스가 같은 group에 가입한 뒤 상태를 확인했다. 일반 실행은 Ctrl+C/SIGTERM 종료도 지원한다.

처리 중 rebalance로 전 세대에서 받은 메시지의 DB 반영이 이미 끝났다면 재전달 가능성은 남는다. 앱이 commit하는 것은 DB 성공한 단일 record뿐이다. 이 단계는 세대 변경과 DB 효과를 원자적으로 묶거나 중복을 차단하지 않는다. 처리 중 강제 rebalance 검증은 수행하지 않았다.

## 11. inventory 설계

```sql
CREATE TABLE inventory (
    product_id BIGINT NOT NULL PRIMARY KEY,
    available_quantity BIGINT NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    CONSTRAINT inventory_positive_product CHECK (product_id > 0),
    CONSTRAINT inventory_nonnegative_quantity CHECK (available_quantity >= 0)
) ENGINE=InnoDB;
```

```sql
UPDATE inventory
SET available_quantity = available_quantity - ?, updated_at = UTC_TIMESTAMP(6)
WHERE product_id = ? AND available_quantity >= ?;
```

조회 후 애플리케이션 계산을 하지 않는다. 같은 상품을 동시 차감해도 DB가 조건 검사와 수량 변경을 한 UPDATE로 수행한다. 명시적 transaction에서 affected rows=1을 확인하고 commit한다. 0행은 상품 없음 또는 재고 부족, 다른 SQL 오류도 반환한다. `orders`·`processed_events`는 없다.

DB에서 음수 UPDATE가 거절되는 것도 rollback하는 별도 schema 검증으로 확인했다. 동시 처리 부하·lock timeout은 이번 실제 검증 범위가 아니다.

## 12. 실제 검증 명령

프로젝트 루트에서 환경을 로드하고 실행했다.

```powershell
. ./scripts/env.ps1
gofmt -w cmd internal tests
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase1.ps1 -Check Build
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase1.ps1 -Check Phase0
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase1.ps1 -Check Flow
```

Build가 실행한 명령: `gofmt -l cmd internal tests`, `go test ./...`, `go vet ./...`, `go build ./...`, `go mod verify`, `git diff --exit-code -- go.mod go.sum`, `go test -tags=integration -run '^$' ./tests/integration`, `go vet -tags=integration ./tests/integration`.

Phase0은 기존 `scripts/verify.ps1 -Check All`로 두 healthcheck, 명시적 topic, Kafka/MySQL smoke, MySQL 재시작 영속성을 확인했다.

Flow는 `go test -tags=integration -count=1 -timeout=4m -v ./tests/integration`이다. 기존 활성 group 또는 pending record가 있으면 seed 전에 중단한다. 문서화된 schema/seed SQL 파일을 그대로 DB에 적용하고 실제 api/worker 실행 파일을 임시 경로에 build하여 별도 프로세스로 시작한다. HTTP는 테스트에서 Go 표준 client로 호출했다.

독립 확인:

```powershell
docker compose ps
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group inventory-main-v1
go list -m all
git diff --exit-code -- go.mod go.sum compose.yaml
git diff --check
```

MySQL CLI로 `SHOW TABLES`, `SELECT product_id,available_quantity,updated_at FROM inventory`, `SHOW CREATE TABLE inventory`를 실행했다. 비밀번호는 컨테이너 환경 변수로 전달하고 출력하지 않았다. Go inspector는 SQL read와 Kafka OffsetFetch를 별도로 수행하므로 로그의 commit 문구만으로 PASS하지 않는다.

## 13. 검증 판정

| 사용자 완료 조건 | 판정 | 증거 |
| --- | --- | --- |
| 정상 HTTP 요청의 Kafka 발행 | PASS | 202 7건과 실제 record의 ID/key/quantity 일치 |
| 실제 Consumer Group 소비 | PASS | inventory-main-v1 Stable 상태와 프로세스 처리 로그 |
| MySQL 정확한 재고 차감 | PASS | 100−19=81, 각 요청별 SQL 확인 |
| DB 성공 후 실제 offset commit | PASS | DB commit→Kafka commit 로그 순서 및 broker OffsetFetch 증가 |
| 정상 재시작 후 이미 commit된 메시지 미처리 | PASS | PID 3052→33184, group 재가입 후 5초 관측, 처리 0·재고/offset 불변 |
| 잘못된 HTTP 입력 미발행 | PASS | 400 5건, 모든 파티션 end/committed offset 불변 |
| Phase 0 검증 유지 | PASS | 기존 전체 smoke·영속성 검사 성공 |
| Phase 2 이상 미구현 | PASS | 소스·실제 topic/schema·의존성 대조 |

추가 검사: format/unit test/vet/build PASS, negative inventory CHECK PASS, DB 실패 시 commit/다음 fetch 미호출 단위 테스트 PASS.

실제 DB 장애나 DB commit 직후 crash, 처리 중 강제 rebalance는 **UNVERIFIED (후속 Phase 범위)**다. 위 8개 정상 흐름 완료 조건에 포함시키거나 검증했다고 주장하지 않는다.

## 14. 실측 수치

| 순서 | quantity | 재고 | partition | record offset | 확인한 committed offset |
| --- | --- | --- | --- | --- | --- |
| seed | — | 100 | — | — | — |
| 1 | 1 | 99 | 0 | 0 | 1 |
| 2 | 3 | 96 | 2 | 0 | 1 |
| 3 | 1 | 95 | 1 | 0 | 1 |
| 4 | 2 | 93 | 0 | 1 | 2 |
| 5 | 3 | 90 | 0 | 2 | 3 |
| 6 | 4 | 86 | 3 | 1 | 2 |
| 7 | 5 | 81 | 2 | 1 | 2 |

정상 HTTP 7건, quantity 합계 19, invalid HTTP 5건. 첫 이벤트는 Worker를 시작하기 전에 API 202와 Kafka record를 확인했으며 그 시점 DB는 여전히 100이었다. 202가 재고 처리 완료가 아님을 실측으로 확인한 것이다.

최종 성공 실행 외에, 앞선 검증 코드 오류 때 발생한 정상 이벤트 1건이 topic에 있다. 해당 record는 partition 3/offset 0, eventId `4b85fd81-9977-4fff-a8b5-ce2ef4205b0d`이며 별도 정상 Worker로 DB/offset 반영을 마쳤다. 따라서 topic 총 record는 8건, 최종 성공 시나리오의 record는 7건이다. 성공 실행은 미처리 record가 없음을 확인한 뒤 seed 100으로 시작했다. 이전 1건을 이번 7건의 수치에 섞지 않았다.

## 15. 실제 group committed offset

| partition | 실행 전 committed / end | 실행 후 committed / end | 정상 재시작 후 |
| --- | --- | --- | --- |
| 0 | -1 / 0 | 3 / 3 | 동일 |
| 1 | -1 / 0 | 1 / 1 | 동일 |
| 2 | -1 / 0 | 2 / 2 | 동일 |
| 3 | 1 / 1 | 2 / 2 | 동일 |
| 4 | -1 / 0 | -1 / 0 | 동일 |
| 5 | -1 / 0 | -1 / 0 | 동일 |

`-1`은 저장된 group offset이 없다는 Kafka 응답이며 0으로 임의 변환하지 않았다. 4·5 파티션은 데이터가 없다. Kafka CLI는 실제 commit 기록이 있는 0~3만 표시했고 각각 lag 0이었다. 전체 여섯 파티션의 값은 OffsetFetch/ReadLastOffset으로 확인했다.

Worker의 새 group 가입이 Stable임을 확인한 뒤 5초(Reader MaxWait=1초 기준 다섯 구간) 관측했다. 새 Worker의 event_received=0, DB=81, offset 불변이다. 이는 정상 종료 후 resume 확인이며 모든 장애 상황의 재전달 방지는 아니다.

## 16. 문제와 해결

| 문제 | 처리 |
| --- | --- |
| Phase 0 precommit의 EOF blank line | 세 파일 끝 빈 줄만 정리 후 staged diff check와 commit 성공 |
| 통합 코드의 상수명 오타 | pinned kafka-go의 `GroupIdNotFound` 명칭에 맞춰 수정하고 compile 확인 |
| 그룹 조회 Not Coordinator | bootstrap에 직접 조회하던 inspector에 FindCoordinator 절차를 추가하고 coordinator 주소로 group API 수행 |
| 검증용 Reader의 MaxBytes 미지정 | MinBytes=1에 맞게 MaxBytes=1e6을 명시. 프로덕션 Worker Reader에는 이미 지정되어 있었음 |
| 검증 오류로 남은 정상 record 1건 | offset reset·재발행 없이 기존 Worker로 처리. DB 반영 후 partition 3 committed=1 확인, stdin 종료로 정상 종료 |
| 로그 버퍼의 동시 접근 가능성 | embedded bytes.Buffer 대신 private 필드와 mutex 메서드만 노출하도록 검토 중 수정 |
| 자동 승인 검토의 사용량 제한 | 검증 명령이 실행 전에 거절되어 잠시 중단. 사용자 재개 요청 후 동일 승인 경로로 실행하여 성공 |

실패한 실행을 PASS로 바꾸어 기록하지 않았다. 수정 후 성공한 최종 실행의 증거를 별도로 보존했다. 새 client·Retry·DLQ·복구 기능으로 문제를 우회하지 않았다.

## 17. 범위 확인

실제 DB 테이블은 `inventory`, 기존 `phase0_probe`뿐이다. 실제 topic은 `orders.created.v1`, 기존 `phase0.smoke.v1`, Kafka group 관리를 위한 내부 `__consumer_offsets`뿐이다. Retry/DLQ/Recovery topic이 없다.

processed_events, eventId UNIQUE 멱등성, Replay, Redis, ORM, Web Framework, Prometheus/Grafana, Fault Injector, Experiment Runner, Kafka Transaction, Outbox는 없다. 테스트 readiness polling과 supervisor의 정상 종료는 업무 Retry나 장애 주입 기능이 아니다.

## 18. Phase 1 commit

완료 조건과 staged 보안·파일·diff 검사를 모두 통과한 상태를 `feat: implement phase 1 order inventory flow` 메시지로 로컬 commit했다. 직후 working tree는 clean이었다. 완료 장부의 최종 확인 기록만 같은 commit에 반영했으며 코드 변경은 없다. 최종 commit hash는 완료 응답과 `git log -1 --format='%H %s'`에서 확인한다. 자기 자신의 hash를 commit 내용에 기록할 수 없으므로 이 파일에는 hash를 삽입하지 않는다. push는 하지 않았다. Phase 1 장부는 **met 7 / unmet 0 / abandoned 0**이다.

## 19. Phase 2 전 주의점

- DB commit과 Kafka offset commit 사이의 종료는 중복 차감을 만들 수 있다. Phase 1은 이 문제를 의도적으로 해결하지 않았다.
- 실패 이벤트가 생기면 Worker가 종료되어 진행이 멈춘다. 다음 Phase에서 원인과 재전달을 재현하되 임의로 offset을 건너뛰지 않는다.
- 정상 재시작 결과를 강제 crash·강제 rebalance의 증거로 사용하지 않는다.
- seed는 product 1의 수량을 100으로 초기화한다. pending record/활성 group이 없는 로컬 환경에서만 사용한다.
- 현재 group의 committed state는 보존한다. 실험 재현 시 원래 offset과 DB 초기 상태를 함께 기록해야 한다.
- API/Worker 테스트 프로세스는 정상 종료했다. Kafka/MySQL 컨테이너와 volume은 유지했다.
- 단일 Worker 순차 처리의 낮은 처리량과 단일 broker의 내구성 한계가 있다. Phase 1은 부하 성능 비교를 수행하지 않았다.

Phase 2는 별도 요청 전까지 진행하지 않는다.
