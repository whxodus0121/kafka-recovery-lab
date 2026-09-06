# kafka-recovery-lab

메시지는 재전달될 수 있지만, 재고 반영은 중복되지 않고, 재시도와 복구 부하는 통제되는 구조를 단계별로 검증하는 프로젝트다.

현재 구현은 **Phase 0: 재현 가능한 로컬 개발 환경**까지다. 위 문장은 최종 목표이며 현재 달성한 기능을 의미하지 않는다.

## Phase 0 계약

| 구분 | 내용 |
| --- | --- |
| 해결할 문제 | Kafka와 MySQL을 동일 설정으로 기동하고 Go에서 실제 연결 검증 |
| 구현 범위 | 단일 KRaft Kafka, InnoDB MySQL, 영속 volume, 명시적 topic 초기화, transport/SQL smoke 코드 |
| 구현하지 않을 것 | API, Order/Inventory 도메인·테이블, Worker, Retry/DLQ/Backoff/Jitter, 멱등성, Replay, 계측·장애·실험 도구 |
| 검증 방법 | 빌드·설정 테스트, 두 healthcheck, 고유 Kafka 메시지 왕복, SELECT, 고유 SQL 행 저장 → 재시작 → 조회 |
| 완료 조건 | [실행 기록](docs/phase-0-verification.md)의 사용자 검증 항목 모두 PASS. 실행하지 않은 항목은 UNVERIFIED |

## 실행 환경

Go 애플리케이션은 호스트에서 실행하며 Compose에는 Kafka와 MySQL 두 서비스만 둔다. Go 컨테이너, Dockerfile, 웹 프레임워크는 필요하지 않다.

| 구성 | 고정 버전 |
| --- | --- |
| Go | 1.26.5 (`go.mod`) |
| Apache Kafka 공식 JVM 이미지 | 4.2.0 |
| MySQL 공식 이미지 | 8.4.8 |
| `github.com/segmentio/kafka-go` | v0.4.51 |
| `github.com/go-sql-driver/mysql` | v1.10.0 |

Docker Desktop의 Linux containers 모드와 Compose v2 이상(`--wait` 지원)이 필요하다. 두 이미지는 태그와 실제 다운로드한 digest를 함께 고정한다. 실제 사용한 Docker/Compose 버전과 이미지 digest는 실행 기록에 남긴다. Go module 경로는 저장소 주소가 아직 없으므로 `kafka-recovery-lab`을 사용한다.

## 시작하기 — PowerShell

프로젝트 루트에서 실행한다. `env.ps1`은 별도 dotenv 라이브러리 없이 `.env`를 현재 PowerShell 프로세스의 환경 변수로 읽는다. 새 쉘에서 직접 `go run`을 할 때는 다시 dot-source한다.

```powershell
. ./scripts/env.ps1 -Init
go mod download
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check All
```

`-Init`은 `.env`가 없을 때만 생성한다. 두 비밀번호를 각각 암호학적 난수로 만들고 출력하지 않는다. 기존 `.env`는 덮어쓰지 않는다. `.env.example`의 비밀번호는 비어 있으며 `.env`는 Git에서 제외된다.

위 전체 검증은 **MySQL 컨테이너를 실제로 재시작한다.** 다음 Phase에서 작업 중인 프로세스가 생기면 개별 검증을 사용한다. 중간 실패 시 즉시 중단하며, 해당 검증을 수정 후 다시 실행한다.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check Build
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check Infrastructure
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check Topics
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check Kafka
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check MySQL
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check Persistence
```

간단한 상태 확인과 중지:

```powershell
docker compose ps
docker compose logs --tail 100 kafka mysql
docker compose stop
docker compose up -d --wait
```

`docker compose down`도 named volume을 보존한다. `docker compose down -v`는 데이터까지 삭제하므로 일반 종료 명령으로 사용하지 않는다.

## 설정과 연결 주소

| 환경 변수 | 예시/역할 |
| --- | --- |
| `KAFKA_BROKERS` | `127.0.0.1:9092`, 호스트 Go의 bootstrap 주소 목록 |
| `KAFKA_PORT` | `9092`, Docker 호스트 포트 |
| `KAFKA_ADVERTISED_HOST` | `127.0.0.1`, 호스트 클라이언트에 반환할 주소 |
| `MYSQL_HOST`, `MYSQL_PORT` | `127.0.0.1`, `3306` |
| `MYSQL_DATABASE`, `MYSQL_USER` | 전용 개발 DB와 비-root 애플리케이션 계정 |
| `MYSQL_PASSWORD`, `MYSQL_ROOT_PASSWORD` | 커밋하지 않는 로컬 비밀번호 |
| `SMOKE_TIMEOUT` | `60s`, Go 검증 전체 제한 시간 |

포트 충돌 시 `.env`에서 Kafka port와 broker 주소를 함께 바꾼다. MySQL port는 Compose와 Go가 공유한다. `.env` 값이 기존 쉘의 동일 환경 변수보다 우선한다. 로더는 확장·실행 없는 제한된 `KEY=value` 형식만 허용하며 공백, 인용부호, `$` 보간을 지원하지 않는다. 생성된 비밀번호는 이 형식에 맞는 hex 문자열이다.

Kafka 내부 통신은 `kafka:19092`, controller는 `kafka:29093`을 사용한다. 외부 listener와 MySQL published port는 호스트 loopback에만 bind한다. 호스트 Go 검증에서 `kafka:19092`를 사용하면 안 된다. Kafka는 로컬 전용 PLAINTEXT이며 외부 배포용 보안 구성은 아니다.

MySQL 환경 변수의 DB·계정 초기화는 **빈 volume의 최초 기동**에 적용된다. `.env` 비밀번호를 바꿔도 기존 DB 계정 비밀번호가 자동 변경되지는 않는다. volume을 유지한 채 자격 증명을 변경하려면 DB 계정 자체를 명시적으로 변경해야 한다.

## Topic과 probe 데이터

| Topic | 파티션 | 복제 | 보관 정책 | 용도 |
| --- | --- | --- | --- | --- |
| `orders.created.v1` | 6 | 1 | delete, 7일 | Phase 1용 준비만 수행; 현재 이벤트 발행 없음 |
| `phase0.smoke.v1` | 1 | 1 | delete, 7일 | Phase 0 전용 메시지 왕복 |

브로커 `auto.create.topics.enable=false`와 Writer `AllowAutoTopicCreation=false`를 명시한다. `-Check Topics`가 공식 Kafka CLI로 topic을 생성한다. 두 번 실행해 재실행 가능성을 검증하고 파티션·복제·leader/ISR·cleanup·retention을 실제 조회한다. 기존 topic 설정이 다르면 실패하며 임의 변경하지 않는다. `docker compose up` 자체는 topic을 만들지 않는다.

Kafka 검증은 현재 끝 offset을 읽고 고유 key/value를 동기 발행한 뒤 해당 내용을 정확히 소비한다. 과거 테스트 메시지로 성공 판정하지 않는다. smoke는 단일 파티션 직접 읽기여서 consumer group을 만들지 않는다. group commit, rebalance, crash recovery와 업무 멱등성은 검증한 것이 아니다. `MaxAttempts=1`은 smoke Writer 호출 설정이며 네트워크 클라이언트의 내부 연결 동작은 프로젝트의 업무 Retry 구현이 아니다.

MySQL은 `database/sql`과 드라이버로 연결한다. driver connector를 사용해 DSN 문자열을 직접 조합하거나 출력하지 않는다. `SELECT 1, VERSION(), @@default_storage_engine`을 확인한다. 영속성 검증만 `phase0_probe`라는 InnoDB 진단 테이블에 고유 행을 삽입한다. 재시작 전후 값·engine을 확인한 후 해당 행 하나만 삭제하며 빈 진단 테이블은 유지한다. 실패 시 행과 출력 ID를 남겨 재조사할 수 있다. Order/Inventory 테이블은 없다.

## 파일 구조

```text
.
├── .env.example
├── .gitignore
├── compose.yaml
├── go.mod
├── go.sum
├── GATES.md
├── README.md
├── cmd/smoke/
│   ├── main.go
│   ├── kafka.go
│   └── mysql.go
├── internal/config/
│   ├── config.go
│   └── config_test.go
├── scripts/
│   ├── env.ps1
│   └── verify.ps1
└── docs/
    └── phase-0-verification.md
```

`.git/`와 비공개 `.env`는 로컬에만 존재한다. 미래 Phase용 빈 디렉터리는 만들지 않았다. `GATES.md`는 검증 증거 장부이며 `unlazy`는 개발 중 사용한 로컬 도구일 뿐 프로젝트 실행 의존성이 아니다. 다른 개발자는 해당 스킬 없이 `verify.ps1`만 실행하면 된다.

## 의존성과 한계

직접 Go 의존성은 두 개다. 표준 라이브러리는 Kafka wire protocol 또는 MySQL driver를 제공하지 않으므로 `kafka-go`와 `go-sql-driver/mysql`이 필요하다. ORM과 HTTP 라이브러리는 없다. 빌드에 포함되는 전이 의존성 세 개 및 모듈 그래프 전체 목록은 [실행 기록](docs/phase-0-verification.md)에 설명한다.

단일 브로커와 replication factor 1은 로컬 재현 비용을 줄이지만 broker HA나 복제 내구성을 검증하지 못한다. `acks=all`도 이 한계를 없애지 않는다. named volume은 컨테이너 생명주기와 데이터 생명주기를 분리하지만 백업은 아니다. Kafka와 MySQL의 원자적 변경도 보장하지 않는다.

Phase 1 진입 전에 host 포트·Docker 메모리 여유·Go module 공개 주소를 확인한다. 다음 구현에서 consumer group의 수동 commit 계약을 별도로 도입하고 검증해야 한다. Phase 0 연결 성공은 향후 Retry/복구/중복 방지의 증거가 아니다.

공식 근거: [Kafka 4.2 단일 노드 예제](https://raw.githubusercontent.com/apache/kafka/4.2.0/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml), [kafka-go v0.4.51](https://github.com/segmentio/kafka-go/releases/tag/v0.4.51), [MySQL Go driver](https://github.com/go-sql-driver/mysql/tree/v1.10.0), [MySQL 8.4.8 릴리스](https://dev.mysql.com/doc/relnotes/mysql/8.4/en/news-8-4-8.html).
