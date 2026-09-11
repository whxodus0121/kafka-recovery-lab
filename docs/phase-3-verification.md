# Phase 3 실행 및 검증 보고

검증일: 2026-09-08 KST. Phase 2 기준 commit `a4f84f35224cc7952817443d1e3a2da8374ed771`. 기존 포트폴리오 문서를 먼저 `17899a5206c6da251e6d4361e4cf008ec666284c` (`docs: document phases 0 through 2`)로 분리 커밋했다.

## 결과 판정

최종 Phase 3 시나리오 **PASS 8 / FAIL 0 / UNVERIFIED 0**. 빌드·정적 검사, Phase 0/1 회귀, 명시적인 Phase 2 baseline A–D, Retry/DLQ topic 생성도 PASS다. 게이트의 마지막 두 항목은 문서 검토와 로컬 commit 결과다.

이 문서는 최종 성공 실행을 표로 표시한다. 최초 초기화 실패 네 건과 성공 16건도 검증 당시 함께 대조했으며, 최신 여덟 건을 아래 표에 사용한다. 해설은 [phase-3-retry-dlq.md](phase-3-retry-dlq.md)에 있다.

| 검증 | 재고 초기→최종 | Main commit | Retry end / commit | DLQ end | 판정 |
| --- | --- | --- | --- | --- | --- |
| Normal | 100→98 | 1 | 0 / -1 | 0 | PASS |
| Poison | 100→98 | 2 | 0 / -1 | 1 | PASS |
| Retryable Recovery | 100→98 | 1 | 1 / 1 | 0 | PASS |
| Retry Exhaustion | 100→100 | 1 | 3 / 3 | 1 | PASS |
| Invalid Metadata | 100→100 | -1 | 1 / 1 | 1 | PASS |
| Domain Rejection | 100→100 | 2 | 0 / -1 | 2 | PASS |
| Retry Publish Failure | 100→100 | -1 | 0 / -1 | 0 | PASS |
| DLQ Publish Failure | 100→100 | -1 | 0 / -1 | 0 | PASS |

`-1`은 broker에서 commit이 없는 상태다. Metadata의 Retry end 1은 직접 주입한 잘못된 record 하나이며 Worker가 새로 Retry를 발행한 것이 아니다. Domain은 재고 부족과 상품 없음 두 건을 처리했다.

## 오류 정책과 처리 계약

| 분류 | 판정 방식 | 처리 |
| --- | --- | --- |
| Retryable | errors.Is/As; 연결 오류, DB deadline, MySQL 1213/1205 및 연결 종료 오류 | 추가 Retry 최대 3회, 소진 시 DLQ |
| Non-Retryable | JSON·event 계약·schema·key 불일치 | 즉시 DLQ |
| Domain Rejection | ErrProductMissing / ErrInsufficientInventory | 즉시 DLQ |
| 잘못된 Retry Metadata | 누락, 중복, count 범위·표기 오류, 시간·좌표 오류 등 | DLQ, count=null, source Header 보존 |
| Unknown / programming | 위에 해당하지 않는 SQL·설정·내부 오류 | Worker 종료, source 미commit |

정상은 DB 성공 후 source commit, 실패 라우팅은 목적지 broker 발행 성공 후 source commit이다. DB 오류를 문자열로 분류하지 않는다. 목적지 발행은 kafka-go 동기 writer, acks=all, MaxAttempts=1, 자동 topic 생성 금지다.

발행 실패 두 건은 자동 생성이 꺼진 존재하지 않는 목적지 topic을 사용했다. Worker의 실제 종료 원인은 모두 `publish failure destination=...missing: [3] Unknown Topic Or Partition`, 종료 코드 1이었다. 외부 broker 조회로 source commit=-1과 목적지 데이터 증가 없음, SQL로 inventory=100을 확인했다.

## Poison Before → After

Phase 2 baseline에서는 P0/O0(schemaVersion 2)에서 Worker가 두 번 종료되고 P0/O1 정상 record를 처리하지 못했다. 이번 Phase 3에서는 다른 대표 poison인 malformed JSON을 P0/O0에 주입하고 정상 record를 P0/O1에 배치했다. O0 raw value/key가 DLQ envelope에 손실 없이 보존되고 O1의 quantity 2가 반영되어 inventory=98, main committed=2가 됐다.

Poison 유형은 다르지만 동일 partition의 선행 영구 오류가 후속 정상 처리를 막는 문제를 비교한다. Phase 2 데이터를 덮어쓰거나 offset을 이동시켜 결과를 만들지 않았다.

## Retry 횟수·시간·Metadata 실측

Exhaustion은 최초 처리 0 이후 추가 시도 1→2→3을 수행하고 마지막 실패를 DLQ로 보냈다. Main 1회와 Retry 3회 모두 DB 연결 실패다. 최종 Retry end/committed=3/3이고 DLQ의 retryCount=3이다. 재고는 100이다.

| 추가 Retry | next-attempt-at (UTC) | original offset | last-error-code |
| --- | --- | --- | --- |
| 1 | 2026-09-08T04:46:32.9888653Z | 0 | DB_CONNECTION |
| 2 | 2026-09-08T04:46:34.9899099Z | 0 | DB_CONNECTION |
| 3 | 2026-09-08T04:46:36.9909304Z | 0 | DB_CONNECTION |

세 Header의 first-failed-at은 모두 `2026-09-08T04:46:30.9888653Z`, original-topic은 `phase3.078f6674-5da9-4145-887f-5d2adba7242b.main`, original-partition=0이다. raw value와 key도 최초 source와 일치했다.

Recovery는 MySQL 재기동 중 불필요하게 다시 실패하지 않도록 검증에서 Fixed Delay 12초를 명시했다. next-attempt-at은 `2026-09-08T04:46:24.3521858Z`, 실제 event_received는 `2026-09-08T04:46:24.3522792Z`이었다. Header 시각 이전 처리가 없음을 비교했으며 이 차이를 scheduling 정밀도 보장으로 해석하지 않는다. 기본값과 Exhaustion의 delay는 2초다.

## 최신 실행 식별자

topic은 `phase3.<eventId>.main|retry|dlq`다. source partition은 모두 0이며 첫 offset은 0이다. 표의 eventId는 각 실험 fixture 식별자다. malformed JSON 자체에는 유효한 eventId가 없고 Poison의 정상 후속 이벤트에 이 ID를 사용했다.

| Scenario | eventId | productId | Worker PID |
| --- | --- | --- | --- |
| Normal | 380dc483-973f-4192-bf66-0d9be174651c | 1788842761118150200 | 28328 |
| Poison | 5533dcb6-d3ce-4600-af2d-4f5f67c57f2a | 1788842763558450800 | 10572 |
| Recovery | e03d0727-4a5f-43b1-b852-0e3ef2e7ac8d | 1788842766620790500 | 11948, 1824 |
| Exhaustion | 078f6674-5da9-4145-887f-5d2adba7242b | 1788842785918342000 | 22708, 26784 |
| Metadata | 6bd06cb2-10ba-41fb-b071-fa51f4ccddc8 | 1788842805072630800 | 23692 |
| Domain | dabbc235-5cd5-46fd-84f7-4d1767e18554 | 1788842806905890300 | 26940 |
| RetryPublishFailure | 984c853a-762e-407a-a58e-7c027a3ef9bf | 1788842809517754100 | 29148 |
| DLQPublishFailure | 1b6ee9e5-ffcb-45f6-8be1-9c234b2fc54a | 1788842821936542700 | 28600 |

테스트 외부의 Kafka CLI로 Poison main current/end=2/2, Recovery retry=1/1, Exhaustion retry=3/3을 다시 조회했다. 모두 활성 멤버가 없었다. MySQL CLI도 위 최종 재고와 일치했고 기존 product 1은 81이었다. 종료 후 Kafka와 MySQL 모두 running/healthy다.

## 회귀와 실행 명령

| 항목 | 실측 | 판정 |
| --- | --- | --- |
| gofmt / go test ./... / go vet ./... / go build ./... / go mod verify | 전체 성공 | PASS |
| integration 태그 compile / vet | 기존 및 Phase 3 성공 | PASS |
| Phase 0 | Kafka/MySQL health, 명시적 topic, 왕복·SQL·MySQL 재시작 영속성 | PASS |
| Phase 1 | product 1788842699527058100, 정상 7건·거절 5건, 합계 19, 100→81, 재시작 추가 처리 0 | PASS |
| Phase 2 A | 복구 후 같은 record 재전달, 최종 98 | PASS |
| Phase 2 B | crash 직후 100, 재전달 후 98 | PASS |
| Phase 2 C | DB commit 후 98, 재전달 후 96 | PASS |
| Phase 2 D | 두 Worker에서 poison 반복, 최종 100 | PASS |
| Topic | inventory.retry.v1 / inventory.dlq.v1, 각각 1 partition·RF=1 | PASS |

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase3.ps1 -Check Build
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase3.ps1 -Check Regression
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase3.ps1 -Check Scenarios
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase3.ps1 -Check Topics

# 최초 초기화 오류 수정 후, 실패했던 4개 시나리오만 재검증
. ./scripts/env.ps1
go test '-tags=integration,phase2,phase3' -count=1 -timeout=4m -v -run '^TestPhase3/(Normal|Recovery|Metadata|RetryPublishFailure)$' ./tests/integration
```

최종 검증은 동일 스크립트의 명령을 순차 실행하고 exit=0·성공 token·출력 SHA-256을 기록했다. Phase 2 정상 경로를 복사하지 않고 같은 Worker에 -phase2-baseline을 전달해 과거 실패 정책을 명시했다. 일반 실행은 Phase 3 정책이다.

회귀 출력은 `bin/phase3-regression.json`으로 분리한 뒤 Phase 3 evidence에 포함했다. 기존 Phase 0–2 verification/evidence와 상세 문서는 수정하지 않았다.

## 발생한 문제

첫 시나리오 실행의 Normal, Recovery, Metadata, RetryPublishFailure는 topic 생성 직후 leader 준비 전에 snapshot을 읽어 `Not Leader For Partition`으로 실패했다. CreateTopics 응답과 partition 가용 시점의 차이를 확인하고 검증 초기화에 제한된 readiness 대기를 추가했다.

처음에는 증거 저장 defer를 snapshot 뒤에 등록했으므로 그 네 건의 record 식별자를 저장하지 못했다. 실행 출력의 scenario·오류를 별도 초기화 실패 목록으로 남기고, 새 run에서는 defer를 먼저 등록했다. 식별자를 추정하거나 성공 실행 수치로 실패를 덮지 않았다. 이후 실패 네 건의 재검증과 최종 전체 여덟 시나리오는 PASS했다.

## 변경 범위와 검증 한계

주요 변경은 Worker 옵션, 공통 Consumer 정책 분기, inventory 오류 구분·routing, 지연 연결 pool, 동기 실패 writer, Phase 2 regression 옵션, Phase 3 테스트/스크립트·문서다. API, 이벤트 비즈니스 payload, migration, Compose, go.mod/go.sum은 유지했다. 새 외부 dependency는 없다.

Deadlock·lock wait timeout·SQL 문법/인증 오류의 분류는 typed error 단위 테스트로 검증했다. 실제 deadlock/lock wait timeout 생성은 UNVERIFIED이며 필수 일시 DB 장애 실험은 실제 MySQL 정지로 수행했다. Retry/DLQ 발행 직후 crash의 중복 발행, 다중 Worker·rebalance·부하·Broker HA도 UNVERIFIED다. 이를 최종 필수 시나리오의 PASS와 섞지 않는다.

Fixed Delay의 head-of-line blocking, 목적지 발행과 source commit 사이 중복 발행, DB commit 결과 불확실성 및 중복 차감은 남아 있다. Phase 4 전에는 이 Fixed Retry 기준선과 처리 순서 제약을 유지한 채 부하 실험 범위를 별도로 정해야 한다. Backoff/Jitter/Retry Storm/Idempotency/Replay/Rate Limiting/추가 인프라는 구현하지 않았다.

요청한 Phase 3 완료 commit은 `feat: add retry and dlq failure handling`이며 최종 hash는 완료 응답과 Git history에서 확인한다. push하지 않는다.
