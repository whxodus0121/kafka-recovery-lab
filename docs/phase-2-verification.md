# Phase 2 — Kafka/MySQL 장애 경계 재현

검증일: 2026-09-08. 기준 커밋: `0f4b730371998b0240aa922678589782fe14e7bd`.

## 검토 요약

기존 티켓 예매 프로젝트에서는 Consumer 실패 메시지를 DLQ로 격리했지만, DB 처리 성공과 Kafka offset commit 사이의 장애는 충분히 검증하지 않았다. 이번 Phase는 그때 남은 의문을 최소 환경에서 재현한다. 장애를 해결하거나 정상 서비스 개발 중 새 문제를 우연히 발견한 단계가 아니다.

정상 API → OrderCreated → Kafka → FetchMessage → InnoDB UPDATE/COMMIT → CommitMessages 흐름을 유지했다. DB commit 전후의 실제 Worker 종료와 MySQL 중단을 통해 DB와 Kafka의 상태가 원자적으로 바뀌지 않음을 검증했다. 중복 차감을 해결하지 않은 상태가 이 Phase의 완료 상태다.

## 장애 주입과 관측

- A: before-db hook에서 대기한 Worker를 둔 상태로 실제 MySQL 컨테이너를 중지하고 exited 상태를 확인한 뒤 표준입력으로 대기를 해제한다. DB 실패로 exit 1. MySQL 복구 후 fault 없는 새 Worker를 시작한다.
- B: UPDATE가 1행을 변경한 뒤 tx.Commit 호출 전에 os.Exit(86). Go defer를 실행하지 않으므로 애플리케이션 Rollback 호출이 아니라 연결 종료에 따른 MySQL의 미완료 transaction 취소를 관측한다.
- C: tx.Commit이 nil을 반환한 뒤, CommitMessages 호출 전에 os.Exit(87). 로그만 보지 않고 별도 SQL 연결로 첫 차감을 확인하고 broker OffsetFetch로 미commit을 확인한다.
- D: API를 우회하여 schemaVersion=2 이벤트와 정상 후속 이벤트를 같은 partition에 직접 발행한다. fault hook 없이 두 Worker 모두 validation 실패로 exit 1하며 offset 0을 반복 수신한다.

fault는 CLI에서 특정 eventId에만 지정한다. 기본 hook은 nil이며 비즈니스 이벤트 계약에 실험 필드를 넣지 않았다. 테스트 프로세스는 Worker 밖에서 SQL SELECT, Kafka OffsetFetch/ReadLastOffset, 직접 partition reader로 관측한다. Worker 로그의 topic/partition/offset 및 payload SHA-256을 별도 reader의 실제 record와 비교한다. 로그는 순서·재전달 식별의 보조 증거이며 DB/broker 상태를 대신하지 않는다.

각 실행은 새 product, 명시적으로 생성한 단일 partition topic, 새 group을 사용한다. 기존 row는 초기화하지 않고 INSERT하며, 각 시나리오 전후 모든 기존 inventory row가 동일한지도 비교한다. group offset reset은 실행하지 않는다. partition reader의 SetOffset은 관측 전용 reader의 읽기 위치 지정이며 consumer group offset 변경이 아니다.

## 실측 결과

아래 표는 최종 PASS 실행과 첫 관측기 실패 및 재현성 확인 실행을 함께 검토해 정리했다. -1은 해당 group에 committed offset이 없음을 뜻하며 offset 0 record를 commit하면 다음 읽기 위치인 1이 저장된다.

| Scenario | 초기 → 장애 직후 → 재시작 후 inventory | committed offset: 전 → 장애 중 → 재시작 후 | 판정 |
| --- | --- | --- | --- |
| A | 100 → DB 중단 중 조회 불가 / 복구 직후 100 → 98 | -1 → -1 → 1 | PASS |
| B | 100 → 100 → 98 | -1 → -1 → 1 | PASS |
| C | 100 → 98 → 96 | -1 → -1 → 1 | PASS |
| D | 100 → 100 → 100 | -1 → -1 → -1 | PASS |

모든 실험 이벤트 quantity는 2이며 record 좌표는 각 고유 topic의 partition 0 / offset 0이다. A–C end offset은 0 → 1, D는 0 → 2다. D의 정상 후속 record는 partition 0 / offset 1, quantity 1이며 두 번의 Worker 실행 모두 해당 record 처리까지 진행하지 못했다.

C는 첫 DB commit의 성공(98), broker offset 미commit(-1), 동일 topic/partition/offset 및 같은 eventId의 재전달, 두 번째 DB side effect(96)를 모두 충족했다. 이는 Kafka의 재전달만으로 외부 DB의 exactly-once 처리가 보장되지 않음을 보여주는 Phase 5 멱등성 적용 전 비교 자료다.

## 완료 조건

| 번호 | 사용자 완료 조건 | 판정 / 근거 |
| --- | --- | --- |
| 1 | Phase 0 회귀 | PASS: 기동·health·명시적 topic·produce/consume·SQL·재시작 후 영속성 |
| 2 | Phase 1 정상 흐름 회귀 | PASS: 정상 HTTP 7건, 수량 합계 19, 새 product 100 → 81, 잘못된 요청 5건 발행 없음, 정상 재시작 처리 0 |
| 3–4 | DB 실패/미commit 및 복구 후 동일 record | PASS: A |
| 5–6 | DB commit 전 crash의 rollback 및 재전달 | PASS: B |
| 7–10 | 결정적 commit 후 crash, 외부 상태, 동일 record 재전달과 중복 side effect | PASS: C |
| 11–12 | Poison 재전달과 같은 partition 후속 메시지 방해 | PASS: D |
| 13 | offset reset 없음 | PASS: 실험 코드·실행 명령 검토, 새 group 사용 |
| 14 | Phase 3 이상 기능 없음 | PASS: 코드 diff와 의존성 검토 |
| 15 | 성공·실패 검증 기록 | PASS: 실행 결과와 판정을 verification에 정리 |
| 16 | Scenario별 판정 | PASS: A/B/C/D 모두 PASS |

최종 완료 조건 16개: PASS 16 / FAIL 0 / UNVERIFIED 0. 이는 최종 조건 판정이며 개발 중 실패 실행이 없었다는 뜻이 아니다. 실행별 판정은 JSON에 따로 보존한다.

## 실행 명령과 문제 해결

프로젝트 루트 PowerShell에서 실행했다.

```powershell
docker compose up -d --wait --wait-timeout 120
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase2.ps1 -Check All
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase2.ps1 -Check A
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase2.ps1 -Check B
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase2.ps1 -Check C
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase2.ps1 -Check D
```

최종 검증은 동일 스크립트의 Build, Regression, A, B, C, D를 순차 실행하며 각 exit 0과 성공 token을 확인했다. Build는 gofmt, go test ./..., go vet ./..., go build ./..., go mod verify 및 integration 태그 컴파일/정적 검사를 수행한다. Regression은 기존 Phase 0 스크립트와 Phase 1 정상 흐름 테스트를 재사용한다.

문제와 해결:

1. 재개 시 Docker engine pipe가 없었다. 설치된 Docker Desktop을 시작하고 기존 named volume을 유지하여 서비스를 기동했다.
2. PowerShell이 쉼표가 있는 Go build tag 인수를 구문 오류로 처리했다. `'-tags=integration,phase2'`로 인용했다. 이 구문 실패는 시나리오 실행 전 발생했다.
3. 첫 A에서 실제 MySQL 중단 후 Worker의 DB 오류와 미commit은 확인했으나, 복구 후 관측용 DB pool의 기존 연결도 끊어져 `invalid connection`으로 검증이 실패했다. 관측기 연결을 명시적으로 닫고 새 연결을 열도록 수정했다. Worker에 재시도나 오류 우회는 추가하지 않았다. 실패 run은 JSON에 FAIL로 남기고, 별도 product/topic/group으로 다시 실행했다.
4. Kafka topic 이름 관련 CLI 경고는 점/밑줄 혼용 시 metric 이름 충돌 가능성에 대한 일반 경고다. 실험 topic은 점만 사용한다. LF/CRLF Git 안내도 공백 오류와 구별했다.

## 변경 파일과 의존성

| 파일 | 역할 |
| --- | --- |
| cmd/worker/main.go | CLI fault 설정, 특정 이벤트 대상, stdin barrier, PID 로그 및 실험 topic 선택 |
| internal/fault/hooks.go / hooks_test.go | 세 가지 최소 장애 경계와 기본 비활성·대상 제한 검사 |
| internal/inventory/consumer.go | DB 전/후 hook, decode 전 record 좌표/hash 관측 |
| internal/inventory/store.go | UPDATE 후 COMMIT 전 hook |
| internal/inventory/consumer_test.go | 기존 정상/실패 경로 테스트를 nil hook 서명에 맞춤 |
| internal/kafka/client.go | Consumer topic 인수화, 기본 topic은 Worker에서 유지 |
| tests/integration/phase1_test.go | 새 product로 회귀 데이터 격리, 증거 출력 경로 선택, 기존 helper 재사용 |
| tests/integration/phase2_test.go | 실제 Worker 두 프로세스·DB·broker로 A–D 검증, 누적 증거 저장 |
| scripts/verify-phase2.ps1 | 단계별 순차 검증 실행 |
| README.md | 현재 Phase와 재현 방법·한계 |
| docs/phase-2-verification.md | 실제 상태, 이벤트, PID와 로그를 정리한 공개 검증 보고서 |

추가 dependency 0개. Go 1.26.5, Kafka 4.2.0, MySQL 8.4.8, kafka-go v0.4.51, go-sql-driver/mysql v1.10.0을 유지한다. 기존 간접 모듈은 edwards25519 v1.2.0(MySQL 인증), klauspost/compress v1.15.9와 pierrec/lz4/v4 v4.1.15(Kafka codec)다. go.mod/go.sum, Compose, API, 이벤트 계약, migration은 변경하지 않았다.

## 핵심 취약점 및 다음 검증

DB와 offset은 별도 시스템의 commit이므로 사이에 종료되면 C처럼 중복된다. 지금의 worker는 한 실패에서 종료되어 D처럼 같은 partition 뒤의 정상 메시지 처리가 막힌다. 해결 전 상태를 의도적으로 보존했다.

Phase 3에서는 영구 오류 격리와 일시 오류 재시도 정책, Phase 5에서는 MySQL transaction 안의 멱등성 처리를 각각 검증할 근거로 사용한다. Retry/DLQ/Backoff/Jitter/Error Classification/processed_events/Idempotency/Replay/Rate Limiting은 구현하지 않았다. Retry Storm 역시 실행하지 않았다.

단일 partition 격리는 결정적 좌표 비교와 과거 데이터 보존을 단순하게 하지만 다중 partition, 다중 Worker, rebalance, Broker HA를 증명하지 않는다. A는 공유 로컬 MySQL 전체를 중지하므로 다른 실행과 병행할 수 없다. 격리 topic과 실패·poison 데이터가 남고 저장 공간을 사용한다. retention 이후 Kafka record가 사라질 수 있으므로 장기 비교 자료는 보존한 JSON을 기준으로 삼는다.

로컬 commit만 수행하며 push하지 않는다. 실제 commit 식별자는 이 문서를 포함한 `git log -1`과 최종 완료 응답을 기준으로 한다.

## 최종 실행 식별자

각 행은 마지막 PASS 실행이다. Kafka 좌표는 아래 topic / partition 0 / offset 0이다.

| Scenario | topic / eventId | orderId | productId | Worker PID → restart PID |
| --- | --- | --- | --- | --- |
| A | phase2.a.df725870-3299-43ef-87dd-de24d344ac87 | 46733061-98bd-4444-813a-b0be1fec305f | 1788832305754284800 | 21464 → 23320 |
| B | phase2.b.e92aac80-0360-46e8-ad22-f2b780c3c682 | 88de909c-9acf-4046-9cf8-b693110e033c | 1788832320924706700 | 2860 → 7612 |
| C | phase2.c.d37c147a-0501-4726-95f9-9821a12ad495 | 53fde173-7a07-46d1-8008-fb75377626e4 | 1788832335945396700 | 7644 → 13316 |
| D | phase2.d.c694ebe0-6d20-4ba8-86fe-a42e3cff95ad | 31be181c-153c-4aee-95ef-d4fcea3088ee | 1788832350540963600 | 5592 → 23272 |

최종 Phase 1 회귀 productId=1788832292656191100, Worker PID=20372 → 21464, 재시작 추가 처리=0. 시나리오 실행 기록은 PASS 8 / FAIL 1 / UNVERIFIED 0이다. FAIL 1은 위에 기술한 최초 A 관측 연결 실패이며 최종 완료 조건과 구분한다.

## 최종 CLI 교차 확인

Kafka CLI에서 A–C는 current/end=1/1이고 활성 멤버가 없다. D는 commit 이력이 없어 CLI 표에 행이 표시되지 않으며, broker OffsetFetch -1과 end=2는 JSON의 직접 조회 결과를 따른다. MySQL CLI에서 역사적 product 1은 여전히 81이다.

~~~text
Scenario A group=phase2-a-df725870-3299-43ef-87dd-de24d344ac87

Consumer group 'phase2-a-df725870-3299-43ef-87dd-de24d344ac87' has no active members.

GROUP                                         TOPIC                                         PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
phase2-a-df725870-3299-43ef-87dd-de24d344ac87 phase2.a.df725870-3299-43ef-87dd-de24d344ac87 0          1               1               0               -               -               -
eventId=df725870-3299-43ef-87dd-de24d344ac87 orderId=46733061-98bd-4444-813a-b0be1fec305f productId=1788832305754284800 PID=21464->23320
Scenario B group=phase2-b-e92aac80-0360-46e8-ad22-f2b780c3c682

Consumer group 'phase2-b-e92aac80-0360-46e8-ad22-f2b780c3c682' has no active members.

GROUP                                         TOPIC                                         PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
phase2-b-e92aac80-0360-46e8-ad22-f2b780c3c682 phase2.b.e92aac80-0360-46e8-ad22-f2b780c3c682 0          1               1               0               -               -               -
eventId=e92aac80-0360-46e8-ad22-f2b780c3c682 orderId=88de909c-9acf-4046-9cf8-b693110e033c productId=1788832320924706700 PID=2860->7612
Scenario C group=phase2-c-d37c147a-0501-4726-95f9-9821a12ad495

Consumer group 'phase2-c-d37c147a-0501-4726-95f9-9821a12ad495' has no active members.

GROUP                                         TOPIC                                         PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
phase2-c-d37c147a-0501-4726-95f9-9821a12ad495 phase2.c.d37c147a-0501-4726-95f9-9821a12ad495 0          1               1               0               -               -               -
eventId=d37c147a-0501-4726-95f9-9821a12ad495 orderId=53fde173-7a07-46d1-8008-fb75377626e4 productId=1788832335945396700 PID=7644->13316
Scenario D group=phase2-d-c694ebe0-6d20-4ba8-86fe-a42e3cff95ad

Consumer group 'phase2-d-c694ebe0-6d20-4ba8-86fe-a42e3cff95ad' has no active members.
eventId=c694ebe0-6d20-4ba8-86fe-a42e3cff95ad orderId=31be181c-153c-4aee-95ef-d4fcea3088ee productId=1788832350540963600 PID=5592->23272
product_id	available_quantity
1	81
1788832084683436300	81
1788832099533031700	100
1788832175889912300	98
1788832191613617300	98
1788832206986075400	96
1788832221809947800	100
1788832292656191100	81
1788832305754284800	98
1788832320924706700	98
1788832335945396700	96
1788832350540963600	100

~~~
