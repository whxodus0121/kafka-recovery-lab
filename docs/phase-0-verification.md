# Phase 0 실행 및 완료 보고

검증일: 2026-09-07 KST. 작업 루트: `project root`.

## 1. Phase 0 결과 요약

Go 호스트 실행 코드와 Kafka/MySQL 두 컨테이너를 구성했다. 사용자 검증 14개 항목을 실제 실행 또는 명시적인 소스·의존성 검사로 확인했다. 최종 판정은 **PASS 14 / FAIL 0 / UNVERIFIED 0**이다. 초기 실패는 13번에 별도로 보존한다.

완료 장부 `GATES.md`는 실행 검증 6개와 수동 소스·문서 검토 2개로 총 8개이며, **met 8 / unmet 0 / abandoned 0**이다.

Phase 1 이상의 구현은 없다. Go 코드의 Kafka 메시지 왕복과 MySQL SQL 실행, 컨테이너 재시작 후 데이터 유지까지 확인했다. 미래 메시지 처리·장애 복구 기능은 이 판정에 포함하지 않는다.

## 2. 생성·수정 파일 목록

빈 폴더에서 시작했으므로 다음 프로젝트 파일 15개는 모두 신규 생성이다.

```text
.env.example
.gitignore
compose.yaml
go.mod
go.sum
GATES.md
README.md
cmd/smoke/main.go
cmd/smoke/kafka.go
cmd/smoke/mysql.go
internal/config/config.go
internal/config/config_test.go
scripts/env.ps1
scripts/verify.ps1
docs/phase-0-verification.md
```

이와 별도로 Git 관리 디렉터리 `.git/`를 초기화하고 커밋 제외된 로컬 `.env`를 생성했다. `.env`에는 해당 로컬 실험 전용으로 생성한 비밀번호가 있고 저장소 파일 목록에는 포함하지 않는다.

## 3. 파일별 역할

| 파일 | 역할 |
| --- | --- |
| `.env.example` | 비밀 값 없는 환경 변수 예시 |
| `.gitignore` | `.env`, 로컬 `bin/`, 로컬 `.unlazy/` 제외 |
| `compose.yaml` | 공식 이미지 태그·digest 고정, 단일 KRaft Kafka, InnoDB MySQL, healthcheck, 두 named volume |
| `go.mod` | Go 버전과 직접·간접 모듈 버전 |
| `go.sum` | 모듈 다운로드 무결성 checksum |
| `GATES.md` | 실제 검증 명령, 판정과 출력 해시 증거 |
| `README.md` | Phase 0 범위·환경 설정·실행·중지·제약 |
| `cmd/smoke/main.go` | 연결 검증 CLI, 제한 시간·취소 처리 |
| `cmd/smoke/kafka.go` | 고유 테스트 메시지 동기 발행과 동일 메시지 소비 |
| `cmd/smoke/mysql.go` | database/sql 연결·SELECT·InnoDB probe 저장/조회/정리 |
| `internal/config/config.go` | 환경 변수 기반 설정과 값 검증 |
| `internal/config/config_test.go` | 잘못된 broker 주소, DB port·password, timeout 검증 테스트 |
| `scripts/env.ps1` | 임의 코드 실행 없는 환경 변수 로더와 최초 로컬 비밀번호 생성 |
| `scripts/verify.ps1` | Phase 0 개별/전체 검증, 명시적 topic 초기화와 MySQL 재시작 검증 |
| `docs/phase-0-verification.md` | 이 보고서 |

## 4. 최종 디렉터리 구조

```text
kafka-recovery-lab/
├── .git/                         # 로컬 Git 관리 정보
├── .env                          # 로컬 전용, ignored
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

빈 미래 디렉터리, vendored 라이브러리, Dockerfile, API 실행 프로그램은 없다.

## 5. 현재 Git 저장소 상태

- 최초 `git rev-parse --show-toplevel`과 `git status`는 not a git repository로 종료했다.
- `git init -b main`을 한 번 수행했다.
- 현재 branch는 `main`, commit은 0개, remote는 없다.
- 신규 프로젝트 파일 15개는 untracked 상태다. stage·commit·push는 수행하지 않았다.
- `git check-ignore -v .env`는 `.gitignore:1:.env`를 반환했다.
- 실제 로컬 비밀번호는 `.env`에만 존재한다. 저장소용 파일에서 동일 값이 없는지도 출력 없이 비교했다.
- sandbox 계정에서 Git ownership 오류가 발생했지만 실제 사용자 권한의 Git 조회는 정상이다. 전역 `safe.directory`는 변경하지 않았다.

## 6. 사용한 Go 버전

`go version go1.26.5 windows/amd64`. `go.mod`의 `go 1.26.5`로 확인할 수 있다. 현재 호스트 Go를 그대로 사용했으며 별도 Go 도구 체인을 설치하지 않았다.

## 7. Kafka 버전

- 컨테이너 CLI 실측: `4.2.0`.
- 이미지: `apache/kafka:4.2.0`.
- 고정 digest: `sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9`.
- 역할: 한 프로세스의 `broker,controller`, node ID 1.
- 실제 생성된 `server.properties`: `auto.create.topics.enable=false`, `controller.quorum.voters=1@kafka:29093`, `log.dirs=/var/lib/kafka/data`.

## 8. MySQL 버전

- `mysqld --version` 실측: `8.4.8`, Linux x86_64, MySQL Community Server.
- Go `SELECT VERSION()` 실측: `8.4.8`.
- 이미지: `mysql:8.4.8`.
- 고정 digest: `sha256:2952e3be7807f06fc18de50b3ea1a632d5c70d63482ff7d7376fe3aa8999babf`.
- `@@default_storage_engine`과 probe 테이블 engine: `InnoDB`.
- 실제 mount: `kafka-recovery-lab_mysql-data` → `/var/lib/mysql`, type `volume`.

## 9. kafka-go 버전

`github.com/segmentio/kafka-go v0.4.51`. 직접 의존성으로 고정했다. 다른 Kafka Go client는 없다. Kafka 4.2.0과의 본 Phase produce/consume 검증은 PASS이며 모든 client API 호환성까지 검증한 것은 아니다.

## 10. MySQL Driver와 필요성

`github.com/go-sql-driver/mysql v1.10.0`. 표준 `database/sql`은 DB 접근 API이고 MySQL wire protocol 드라이버를 포함하지 않으므로 필요하다. `mysql.NewConnector`와 `sql.OpenDB`를 사용했다. ORM은 없다. [드라이버 공식 설명](https://github.com/go-sql-driver/mysql/tree/v1.10.0).

## 11. 실제 실행한 검증 명령

아래는 프로젝트 루트에서 실행했다. `verify.ps1 -Check All`은 종료 코드 0으로 완료했다. 이후 초기 실패로 미완료였던 G2–G6에 대해서도 개별 명령을 gate checker로 실행해 출력 해시를 남겼다. G2 재검증 중 발견된 JSON 파싱 문제를 수정한 후에는 변경된 Infrastructure 검사만 다시 실행했다. 다른 검증 경로는 수정하지 않았다.

```powershell
go version
docker version
docker compose version
git rev-parse --show-toplevel
git status --short --branch
git remote -v
git check-ignore -v .env
git diff --check

. ./scripts/env.ps1 -Init
go mod tidy
go list -m all
go list -deps -f '{{if .Module}}{{.Module.Path}} {{.Module.Version}}{{end}}' ./...
go mod why -m github.com/xdg-go/scram golang.org/x/net github.com/stretchr/testify

powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check All
```

검증 스크립트가 실제 실행한 핵심 명령:

```powershell
gofmt -l cmd internal
go mod verify
go test ./...
go vet ./...
go build ./...
docker compose config --quiet
docker compose config --services
docker compose up -d --wait --wait-timeout 180
docker compose ps --format json
# 최종 Infrastructure 검사에서는 필요한 필드만 조회한다.
docker compose ps -q
# container ID별: docker inspect --format '{{.State.Status}} {{.State.Health.Status}}' ID
# container ID별: docker inspect --format '{{.Config.Image}}' ID
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --version
docker compose exec -T mysql mysqld --version

# 각각 두 번 생성 요청 후 실제 설정 확인
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic orders.created.v1 --partitions 6 --replication-factor 1 --config cleanup.policy=delete --config retention.ms=604800000
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic phase0.smoke.v1 --partitions 1 --replication-factor 1 --config cleanup.policy=delete --config retention.ms=604800000
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic orders.created.v1
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic phase0.smoke.v1

go run ./cmd/smoke kafka
go run ./cmd/smoke mysql
go run ./cmd/smoke mysql-write 707be59fdadd4af0871046095ef4617b
go run ./cmd/smoke mysql-read 707be59fdadd4af0871046095ef4617b
docker compose restart mysql
# docker inspect의 StartedAt 변경과 health=healthy를 확인한 뒤
go run ./cmd/smoke mysql-read 707be59fdadd4af0871046095ef4617b
go run ./cmd/smoke mysql-clean 707be59fdadd4af0871046095ef4617b
```

위 probe ID는 전체 검증 실행에서 관측한 실제 ID이며 재실행하면 새 ID가 생성된다. 최초 전체 검증의 핵심 출력:

```text
BUILD_PASS
kafka: running healthy
mysql: running healthy
INFRASTRUCTURE_PASS
TOPICS_PASS
KAFKA_PRODUCE_PASS topic=phase0.smoke.v1 key=phase0-57232e69a23db91dc2ce8dfc9aa9fbc4
KAFKA_PASS topic=phase0.smoke.v1 partition=0 offset=0 key=phase0-57232e69a23db91dc2ce8dfc9aa9fbc4 payload_match=true
MYSQL_PASS select=1 version=8.4.8 default_engine=InnoDB
MYSQL_WRITE_PASS id=707be59fdadd4af0871046095ef4617b
MYSQL_READ_PASS id=707be59fdadd4af0871046095ef4617b value_match=true engine=InnoDB
MySQL restarted: before=2026-09-06T18:30:59.054096505Z after=2026-09-06T18:31:29.974242461Z
MYSQL_READ_PASS id=707be59fdadd4af0871046095ef4617b value_match=true engine=InnoDB
MYSQL_CLEAN_PASS id=707be59fdadd4af0871046095ef4617b deleted=1
PERSISTENCE_PASS id=707be59fdadd4af0871046095ef4617b
```

추가 실제 조회: `server.properties`의 KRaft/자동 생성 설정, `SHOW TABLES`, `SELECT COUNT(*) FROM phase0_probe`, `kafka-get-offsets.sh`의 orders topic offset, `docker inspect`의 MySQL mount, `docker image inspect`의 RepoDigests. 민감한 환경 변수 전체나 실제 password는 출력하지 않았다.

개발 도구 버전: Docker Desktop 4.60.1, Engine/CLI 29.2.0, Compose v5.0.2. 기존 설치를 사용했다. 게이트 실행에는 로컬 unlazy 스킬의 `gate-lint.mjs`, `gate-check.mjs --approve --timeout 600 GATES.md`를 사용했다. 이 스킬은 다른 개발자의 프로젝트 실행에 필요하지 않다.

## 12. 사용자 검증 항목별 최종 판정

| 번호 | 검증 항목 | 판정 | 실제 근거 |
| --- | --- | --- | --- |
| 1 | Docker Compose 정상 실행 | PASS | 최종 digest 구성의 `up -d --wait` exit 0 |
| 2 | Kafka 정상 기동 | PASS | running, CLI version 4.2.0 |
| 3 | MySQL 정상 기동 | PASS | running, version 8.4.8 |
| 4 | Kafka healthcheck | PASS | Kafka protocol topic 조회, healthy |
| 5 | MySQL healthcheck | PASS | 앱 계정 TCP 인증 후 SELECT 1, healthy |
| 6 | Go Kafka 연결 | PASS | kafka-go DialLeader와 실제 끝 offset 조회 |
| 7 | 실제 Produce | PASS | 동기 Writer, acks=all, 고유 key 발행 성공 |
| 8 | 실제 Consume | PASS | 동일 key/value, partition 0, offset 0 |
| 9 | Go MySQL 연결 | PASS | database/sql PingContext 성공 |
| 10 | SQL 실행·결과 | PASS | SELECT=1, VERSION=8.4.8, InnoDB |
| 11 | 재시작 후 데이터 유지 | PASS | StartedAt 변경, 동일 ID/값 조회 성공, 마지막에 1행 정리 |
| 12 | orders topic 명시적 준비 | PASS | 재실행 가능한 CLI 생성, 6 partitions, RF=1, delete, 7일, 전체 leader/ISR=1 |
| 13 | 불필요 서비스·라이브러리 부재 | PASS | Compose 두 서비스, 직접 모듈 2개, runtime 전이 3개와 전체 그래프 확인 |
| 14 | Phase 1 이상 기능 부재 | PASS | 소스 검토; DB는 진단 테이블만, orders topic 여섯 파티션 end offset 모두 0 |

`go test`는 설정 검증 테스트이고 Kafka/MySQL 통합 검증은 실제 CLI 실행으로 별도 수행했다. 표의 PASS를 부하 테스트나 장애 복구 성공으로 확대 해석하지 않는다.

## 13. 발생한 문제와 해결

| 문제 | 관측 | 처리와 재검증 |
| --- | --- | --- |
| Docker 엔진 미기동 | named pipe not found | 기존 Docker Desktop을 background 실행, Linux engine 연결 확인 |
| sandbox 권한 제한 | Docker config access denied, Go 다운로드 socket 금지, Git unsafe repository | 필요한 명령을 실제 사용자 권한으로 재실행; 전역 Git 예외나 보안 설정은 변경하지 않음 |
| 존재하지 않는 MySQL 이미지 | `mysql:8.4.12: not found` | 릴리스 문서와 실제 registry tag 제공 여부가 달랐음. manifest가 존재하는 8.4.8로 고정하고 digest도 고정 |
| 초기 G2–G6 실패 | 이미지 pull 실패로 컨테이너·topic 미기동, 연결 거부 | 원인 해결 후 전체 스크립트 exit 0 및 미완료 게이트 재실행; 최초 빌드 G1은 이미 PASS였음 |
| 상태 JSON 파싱의 간헐 실패 | G2 재실행에서 PowerShell `ConvertFrom-Json` ArgumentException; 두 컨테이너는 healthy, 직접 재실행은 PASS | 전체 Compose JSON 파싱을 제거하고 `docker inspect --format`으로 필요한 ASCII 상태 필드만 직접 비교. 실패 당시 입력이 보존되지 않아 문자 인코딩이 직접 원인이었는지는 확인 불가. 변경된 G2를 재검증 |
| Kafka topic 명명 경고 | `.`와 `_` 혼용 시 metric name 충돌 가능성 안내 | 현재 topic은 `.`만 사용하므로 설정 유지; 경고를 숨기지 않음 |

오류를 무시하거나 클라이언트를 바꾸지 않았다. 이미지 교체는 같은 MySQL 8.4 계열에서 실제 제공되는 패치를 선택한 것이다. 최신 보안 패치 비교 또는 취약점 전수 점검은 본 Phase에서 수행하지 않았다.

## 14. 외부 의존성 전체 목록

애플리케이션 빌드에 포함되는 Go 모듈은 다음 다섯 개다. 앞의 두 개만 직접 선택한 의존성이다.

| 모듈 | 버전 | 구분 |
| --- | --- | --- |
| `github.com/segmentio/kafka-go` | v0.4.51 | 직접 |
| `github.com/go-sql-driver/mysql` | v1.10.0 | 직접 |
| `filippo.io/edwards25519` | v1.2.0 | MySQL driver 전이 |
| `github.com/klauspost/compress` | v1.15.9 | Kafka client 전이 |
| `github.com/pierrec/lz4/v4` | v4.1.15 | Kafka client 전이 |

`go list -m all`에 나타나지만 현재 애플리케이션의 `go list -deps`에 포함되지 않는 모듈도 생략하지 않는다. 아래는 의존 라이브러리의 테스트·선택적 인증 관련 모듈 그래프이며, 프로젝트가 별도 기능으로 도입한 라이브러리가 아니다. `go mod tidy`는 해당 테스트 의존성 checksum도 `go.sum`에 남긴다.

| 모듈 | 버전 |
| --- | --- |
| `github.com/davecgh/go-spew` | v1.1.1 |
| `github.com/pmezard/go-difflib` | v1.0.0 |
| `github.com/stretchr/testify` | v1.8.0 |
| `github.com/xdg-go/pbkdf2` | v1.0.0 |
| `github.com/xdg-go/scram` | v1.1.2 |
| `github.com/xdg-go/stringprep` | v1.0.4 |
| `golang.org/x/net` | v0.38.0 |
| `golang.org/x/text` | v0.23.0 |
| `gopkg.in/yaml.v3` | v3.0.1 |

직접 및 전체 모듈 그래프를 합쳐 외부 Go 모듈은 14개다. 인프라 이미지는 앞서 명시한 `apache/kafka`, `mysql` 두 개다. 도구는 기존 Go, Git, Docker Desktop/Compose, PowerShell을 사용한다. 새 인프라 제품·웹 프레임워크·ORM은 추가하지 않았다.

## 15. 의존성별 필요성

| 의존성 | 이유 |
| --- | --- |
| kafka-go | 지정된 Go Kafka protocol client, 실제 produce/consume |
| go-sql-driver/mysql | database/sql을 MySQL protocol에 연결하는 driver |
| edwards25519 | 선택한 MySQL driver에 포함된 인증 연산 지원 |
| klauspost/compress | Kafka client가 포함하는 압축 codec 지원 |
| pierrec/lz4 | Kafka LZ4 codec 지원 |
| testify | Kafka client 자체 테스트의 assertion 도구; 프로젝트 테스트는 표준 testing만 사용 |
| go-spew | testify의 진단 값 표시 |
| go-difflib | testify의 차이 출력 |
| scram | Kafka client의 선택적 SCRAM 인증 관련 테스트 경로 |
| pbkdf2 | SCRAM key derivation |
| stringprep | SCRAM 입력 정규화 |
| x/net | Kafka client 자체 테스트의 nettest 등 |
| x/text | stringprep의 Unicode 정규화 |
| yaml.v3 | testify의 YAML 비교 지원 |
| apache/kafka 이미지 | 단일 KRaft Kafka broker/controller 실행 |
| mysql 이미지 | InnoDB 영속 DB 실행 |
| Go | 호스트 프로그램 build/run과 표준 라이브러리 |
| Git | 프로젝트 버전 관리 |
| Docker Desktop/Compose | Linux containers와 로컬 서비스·volume 관리 |
| PowerShell | 현재 Windows 환경에서 환경 변수와 재현 명령 실행 |

## 16. Phase 0 범위 초과 여부

**PASS — 초과 구현 없음.** API·Order 도메인·Inventory Worker와 업무 테이블, Retry/DLQ/backoff/jitter/오류 분류/멱등성/Replay/Recovery/Rate Limit, Prometheus/Grafana, Fault Injector/Experiment Runner를 구현하지 않았다. Redis/PostgreSQL/Kubernetes/Elasticsearch/MongoDB/Schema Registry/다른 broker/ORM/웹 프레임워크도 없다. Kafka Transaction, Outbox, Circuit Breaker, Distributed Rate Limiter도 없다.

`scripts/verify.ps1`은 Phase 0 연결·영속성 확인을 위한 스크립트이며 부하나 장애 전략을 비교하는 Experiment Runner가 아니다. `phase0_probe`의 기본 키는 고유한 진단 행을 구별할 목적이며 업무 메시지 멱등성 구현이 아니다.

## 17. Phase 1 전 주의점

1. 단일 broker·RF=1은 로컬 재현용이다. broker HA와 복제 내구성은 검증하지 않았다.
2. Go는 호스트 실행이다. 컨테이너 내부 listener `kafka:19092`와 호스트 listener `127.0.0.1:9092`를 혼동하지 않는다.
3. MySQL의 기존 named volume에는 기존 계정 비밀번호가 남는다. `.env` 변경만으로 DB 계정이 갱신되지 않는다.
4. `-Check Persistence` 또는 `-Check All`은 MySQL을 재시작한다. 다음 Phase에서 작업 중인 트랜잭션이 생기면 실행 시점을 통제한다.
5. Kafka 그룹 소비·수동 offset commit·rebalance는 다음 Phase에서 별도 구현·검증해야 한다. 현재 smoke는 partition 직접 읽기다.
6. 아직 실제 저장소 원격 주소가 없어 Go module은 로컬 이름이다. 원격 저장소를 정하면 필요시 import prefix를 함께 변경한다.
7. 기존 9092/3306 사용 여부, Docker의 CPU·메모리 여유를 확인한다. 현재 두 서비스는 healthy 상태로 실행 중이며 volume은 보존했다.
8. 이미지와 module 버전은 재현성을 위해 고정했다. 향후 버전 갱신 시 본 검증을 다시 수행해야 한다.

Phase 1은 별도 요청 전까지 진행하지 않는다.
