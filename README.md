# kafka-recovery-lab

메시지는 재전달될 수 있지만, 재고 반영은 중복되지 않고, 재시도와 복구 부하는 통제되는 구조를 단계별로 검증하는 프로젝트다. **현재는 Phase 1 정상 흐름까지 구현**했다. 중복 차감 방지와 장애 복구는 아직 없다.

## 현재 범위

```text
POST /orders → OrderCreated → orders.created.v1
             → inventory-main-v1 Consumer → MySQL inventory 차감 → offset commit
```

| 항목 | Phase 1 계약 |
| --- | --- |
| 문제 | HTTP 요청을 Kafka 이벤트로 전달하고 재고에 정확한 수량을 반영 |
| 구현 | net/http API, UUID v4 이벤트, kafka-go Producer/Consumer Group, InnoDB 재고, 수동 commit |
| 제외 | 업무 Retry/DLQ/Backoff/Jitter, 멱등성, Replay, 계측, 장애 주입, Outbox/트랜잭션 Kafka |
| 검증 | 실제 별도 API/Worker 프로세스, HTTP·Kafka 레코드·DB 수량·브로커 offset 대조와 정상 재시작 |
| 완료 조건 | [Phase 1 보고서](docs/phase-1-verification.md)의 8개 완료 조건 모두 PASS |

[Phase 0 기록](docs/phase-0-verification.md)과 `GATES.md`는 기준 커밋 시점의 역사적 기록이다. 현재 게이트는 `PHASE1_GATES.md`, 실제 이벤트·offset·프로세스 로그는 [JSON 증거](docs/phase-1-evidence.json)에 있다.

## 환경

Go 프로그램은 호스트에서 실행한다. Compose 서비스는 Kafka와 MySQL 두 개다.

| 구성 | 버전 |
| --- | --- |
| Go | 1.26.5 |
| Kafka | 4.2.0, 공식 이미지 태그와 digest 고정 |
| MySQL | 8.4.8, 공식 이미지 태그와 digest 고정 |
| github.com/segmentio/kafka-go | v0.4.51 |
| github.com/go-sql-driver/mysql | v1.10.0 |

Go, PowerShell, Docker Desktop의 Linux engine 및 `--wait`를 지원하는 Compose가 필요하다. Phase 1에 새 외부 모듈·인프라를 추가하지 않았다. `go.mod`와 `go.sum`은 Phase 0과 같다. 표준 `database/sql`에 MySQL protocol driver가 필요하므로 기존 드라이버를 공유한다. ORM·HTTP 프레임워크·다른 Kafka client는 없다.

## 처음 시작하기 — 프로젝트 루트 PowerShell

```powershell
. ./scripts/env.ps1 -Init
go mod download
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check Infrastructure
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check Topics
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/init-inventory.ps1 -Seed
```

`-Init`은 없는 `.env`만 생성하며 로컬 MySQL 비밀번호 두 개를 각각 난수로 만든다. `.env`는 커밋하지 않는다. 로더는 보간이나 실행 없는 제한된 `KEY=value` 형식만 지원한다. 기존 Phase 0 `.env`도 사용할 수 있다.

`-Seed`는 **product 1 재고를 100으로 명시적으로 되돌린다.** 기존 API/Worker를 중지하고 미처리 Kafka 메시지가 없는 로컬 검증 환경에서만 실행한다. `-Seed`를 생략하면 스키마 생성만 수행한다. schema SQL과 seed SQL은 별도 파일이며 migration framework는 없다.

별도 터미널 두 개에서 실행한다.

```powershell
# 터미널 A
. ./scripts/env.ps1
go run ./cmd/worker
```

```powershell
# 터미널 B
. ./scripts/env.ps1
go run ./cmd/api
```

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8080/orders -ContentType application/json -Body '{"productId":1,"quantity":1}'
```

Ctrl+C로 정상 종료한다. 자동 검증의 `-shutdown-on-stdin-close` 옵션은 로컬 부모 프로세스가 stdin을 닫아 같은 context 취소 경로로 정상 종료시키기 위한 것으로, 기본 실행에서는 사용하지 않는다. 공개 종료 API나 장애 주입 기능은 없다.

## API·이벤트 계약

`POST /orders`, Content-Type `application/json`. 양의 int64 `productId`, `quantity`가 필수다. 4096바이트 제한, 알 수 없는 필드·JSON 뒤 추가 데이터·비정수·누락·0/음수는 400으로 거절한다. 미지원 Content-Type은 415, 미지원 method는 405다.

동기 Kafka 발행 성공 후 `202 Accepted`와 `status=accepted`, `eventId`, `orderId`를 반환한다. 202는 재고 반영 완료가 아니다. 발행 결과를 확인하지 못하면 503을 반환하며 결과가 불확실할 수 있음을 알린다. HTTP 재요청의 중복 발행을 막지 않는다.

```json
{
  "schemaVersion": 1,
  "eventId": "640c38ca-469b-4843-a5f1-348e9889dd7e",
  "orderId": "dbf7cafa-a3cd-4abc-86ff-b2b27eb50a17",
  "productId": 1,
  "quantity": 1,
  "createdAt": "2026-09-07T01:00:00Z"
}
```

위 JSON은 형식 예시다. 실제 값은 증거 JSON을 참조한다. ID는 crypto/rand로 만든 UUID v4, createdAt은 UTC다. Topic은 `orders.created.v1`, key는 `orderId`. Producer는 Hash balancer, acks=all, 동기 발행, `MaxAttempts=1`, 자동 topic 생성 금지를 사용한다. topic은 명시적 CLI로 6 partitions/RF=1/delete/7일 retention으로 준비한다.

Consumer는 기본 `inventory-main-v1` Group으로 `FetchMessage`를 호출한다. `CommitInterval=0`은 명시적 `CommitMessages`의 동기 commit 설정이다. 자동 commit하는 `ReadMessage`를 쓰지 않는다. 그룹에 저장된 offset이 없으면 처음부터 읽는다.

애플리케이션은 전 파티션을 합쳐 한 번에 한 메시지만 처리한다. `event_received → inventory_committed → offset_committed` 로그에 eventId/orderId/productId와 partition/offset을 남긴다. DB·validation·commit 실패 시 다음 메시지로 진행하지 않고 Worker가 종료된다. 따라서 실패한 메시지를 건너뛰는 후속 commit이 없다. 대신 한 실패가 이 Worker 전체 처리를 중단하는 비용이 있다.

재고는 InnoDB 트랜잭션에서 조건부 원자 UPDATE로 차감한다. 성공한 row가 정확히 1개이고 DB transaction commit이 성공한 뒤 Kafka commit한다. 0행이면 상품 없음 또는 재고 부족으로 처리하며 아직 둘을 별도 복구 정책으로 분류하지 않는다. DB CHECK 제약도 음수를 금지한다.

## 설정

Phase 0 변수는 `.env.example`을 참조한다. 추가 선택 변수는 `API_ADDR`(기본 `127.0.0.1:8080`)와 `KAFKA_CONSUMER_GROUP`(기본 `inventory-main-v1`)뿐이다. 직접 쉘 환경 변수로 설정할 수도 있다. `.env`에 같은 키가 있으면 로더가 그 값으로 덮어쓴다.

호스트 Kafka 주소는 `127.0.0.1:9092`, 내부 주소는 `kafka:19092`다. Kafka 포트를 바꾸면 `KAFKA_PORT`와 `KAFKA_BROKERS`를 함께 바꾼다. MySQL은 `127.0.0.1:3306`이다. 두 published port는 loopback에만 bind하며 Kafka PLAINTEXT는 로컬 실험용이다.

MySQL named volume 초기화는 첫 기동에만 계정을 만든다. `.env` 비밀번호를 바꿔도 기존 DB 계정은 자동 변경되지 않는다. 데이터는 `docker compose stop`과 `docker compose down` 후에도 남지만 `down -v`는 volume까지 삭제한다.

## 검증

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase1.ps1 -Check Build
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase1.ps1 -Check Phase0
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase1.ps1 -Check Flow
```

`-Check All`은 위 순서대로 실행한다. Phase0 검사는 MySQL을 실제 재시작하므로 먼저 애플리케이션을 중지한다. Flow 검사는 활성 Consumer Group 또는 미처리 record가 있으면 seed 전에 실패하며 offset을 강제로 바꾸지 않는다. 검증 시작 시 product 1을 100으로 seed하고, API/Worker를 임시 실행 파일로 띄운다. 검증이 성공하면 재고는 81이며 새 이벤트와 group offset은 Kafka에 남는다. 재실행 시 새로운 baseline에서 검증하고 `docs/phase-1-evidence.json`을 갱신한다.

일반 `go test ./...`는 단위 테스트만 실행한다. 실제 서비스 검증은 `go test -tags=integration -count=1 -timeout=4m -v ./tests/integration`이다. 테스트 내부는 별도 실제 프로세스를 사용한다. Go module 추가 없이 Windows 프로세스 정상 종료를 확인한다.

## 한계와 다음 Phase 경계

DB commit과 Kafka offset commit은 원자적이지 않다. 그 사이 종료·rebalance·commit 실패가 발생하면 같은 이벤트가 재전달되어 재고가 다시 차감될 수 있다. 멱등성은 의도적으로 아직 없다. 정상 재시작 시험의 재처리 0건은 중복 메시지 방지 보장을 의미하지 않는다.

이번 실제 검증은 정상 흐름과 정상 종료/재시작이다. DB 장애, 처리 중 강제 rebalance, DB commit 직후 crash, 동시 다중 Worker의 장애 내구성은 미검증이다. DB 실패 시 commit 미호출은 단위 테스트로 별도 확인했다. 단일 broker/RF=1의 HA·복제 내구성도 보장하지 않는다.

seed는 local reset이며 업무 기능이 아니다. Phase 2의 장애 재현 또는 그 이후 Retry/DLQ/Idempotency/Replay는 별도 요청 전 구현하지 않는다. 전체 파일 구조·변경 역할·실측 수치는 [Phase 1 보고서](docs/phase-1-verification.md)에 있다.
