# Phase 4 검증 결과

실행일: 2026-09-08. 필수 검증 **PASS 15 / FAIL 0 / UNVERIFIED 0**. 비교용 6개 셀은 모두 증거 대조를 통과했다. 실행 원본은 7개이며, 첫 Fixed A는 실제 장애 길이가 비교 범위를 벗어나 제외한 뒤 같은 설정으로 다시 실행했다. 제외 원본과 초기 회귀 실패도 보존했다.

이 판정은 실험과 계산의 검증 결과다. Jitter의 일반적 우월성이나 완전히 동일한 실제 DB 재기동 시간까지 입증했다는 뜻은 아니다. 계획된 입력·설정·복구 요청 시점은 같으며, 동일하게 DB가 중단된 최초 8초를 공통 비교 구간으로 사용했다. 전체 복구 비교에는 아래 실제 장애 길이 편차가 남는다.

## 실험 조건

| 항목 | 공통 설정 |
| --- | --- |
| 이벤트 / 상품 | 60건, 상품 6개에 round-robin, quantity 1 |
| 초기 재고 | 상품별 100, 실행별 새 productId, 기존 행 변경 없음 |
| Topic / group | 실행마다 Main/Retry/DLQ 및 group 격리, topic별 partition 1, RF 1 |
| Worker / DB pool | Main 1 + Retry 1, 각 pool 최대 연결 1 |
| Retry | 추가 최대 5회, base 1초, cap 8초; 기본 서비스 Fixed 2초/3회는 별도 회귀 |
| RNG seed | Main 4101, Retry 4102; 실제 Header를 난수열과 대조 |
| Timeout | DB 작업 10초, DB 연결 5초, Kafka 발행/commit 10초 |
| 장애 | 실제 `docker compose stop mysql`, 실패한 외부 SQL 확인 후 t=0 |
| 복구 / 관측 | t=8초에 `docker compose start mysql` 요청, 35초 관측 |
| 관측 간격 | offset 목표 500ms, 외부 SQL 복구 확인 100ms 루프, SQL probe timeout 200ms |
| A | 지연 없는 동기 ACK 연속 발행. 60건 모두 최초 DB 장애를 만남 |
| B | 목표 5건/초, 200ms 간격 지속 유입. 장애 중 Main과 Retry가 함께 동작 |

A의 실제 발행 구간은 Fixed 0.078초, Exponential 0.101초, Jitter 0.231초였다. B는 각각 11.803 / 11.807 / 11.801초이며 최대 발행 시작 지각은 1.589 / 1.413 / 1.584ms였다. 같은 발행 계획이 실제로 완전히 동일한 타이밍을 만든다고 가정하지 않았다.

## 전략별 실제 결과

RPS는 DB query나 TCP 연결 시도 수가 아닌 **애플리케이션 Retry 시작 횟수**다. t=0 기준 1초 bin으로 계산했으며, 검증 당시 전체 원시 시작 시각과 0 bin을 포함해 집계했다.

| 시나리오 / 전략 | 전체 peak RPS | 전체 Retry | 공통 8초 Retry / peak | 성공 | DLQ | 미완료 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A Fixed | 60 | 300 | 300 / 60 | 0 | 60 | 0 |
| A Exponential | 60 | 240 | 180 / 60 | 60 | 0 | 0 |
| A Full Jitter | 44 | 225 | 164 / 44 | 60 | 0 | 0 |
| B Fixed | 25 | 239 | 125 / 25 | 28 | 32 | 0 |
| B Exponential | 35 | 113 | 37 / 15 | 60 | 0 | 0 |
| B Full Jitter | 39 | 96 | 38 / 15 | 60 | 0 | 0 |

성공 이벤트의 product별 quantity 합계를 최종 SQL 재고와 대조했다. A Fixed는 총 재고 600 유지, B Fixed는 600→572, 나머지는 600→540이었다. DLQ 원본과 성공 eventId의 교집합은 없고 각 셀에서 `성공 + DLQ + 미완료 = 60`이었다. 최종 Main/Retry committed offset은 각 end offset과 일치했다. DLQ는 수신 프로그램이 없으므로 소비 완료로 주장하지 않는다.

| 시나리오 / 전략 | 실제 장애 길이(s) | 처리 종료(s)¹ | 전량 성공 복구(s)¹ | overdue p95 / max(ms) | HOL 확인 attempt |
| --- | ---: | ---: | ---: | ---: | ---: |
| A Fixed | 9.883 | -3.818 | 미달성 | 80.4 / 82.9 | 0 |
| A Exponential | 10.439 | 5.651 | 5.651 | 26.9 / 32.6 | 0 |
| A Full Jitter | 10.432 | 4.634 | 4.634 | 5698.6 / 6956.3 | 196 |
| B Fixed | 11.750 | 2.848 | 미달성 | 2088.6 / 2405.9 | 0 |
| B Exponential | 10.943 | 5.654 | 5.654 | 6815.4 / 6899.4 | 89 |
| B Full Jitter | 10.988 | 4.093 | 4.093 | 5684.4 / 5963.5 | 85 |

¹ 외부 SQL로 확인한 장애 종료 시각을 기준으로 한다. 처리 종료는 마지막 terminal DB commit/DLQ 이후 Main/Retry offset이 모두 따라잡힌 첫 sample 완료 시각이며 sampling 상한이다. A Fixed의 음수는 **DB 복구 3.818초 전에 전부 DLQ로 종료**됐다는 뜻이다. 전량 성공 복구는 성공 60건과 offset drain이 모두 필요하며, 미달성은 `null`로 기록했다.

| 시나리오 / 전략 | Main / Retry peak lag | 최초 실패 이벤트 수 | 예약 시각 역전 수 |
| --- | ---: | ---: | ---: |
| A Fixed | 25 / 60 | 60 | 0 |
| A Exponential | 24 / 60 | 60 | 0 |
| A Full Jitter | 26 / 60 | 60 | 113 |
| B Fixed | 2 / 26 | 58 | 0 |
| B Exponential | 1 / 55 | 55 | 34 |
| B Full Jitter | 2 / 54 | 54 | 42 |

lag는 `max(0, end - max(0, committed))`다. 초기 committed -1은 미commit으로 보존하고 계산에만 0을 사용했다. Broker snapshot은 순차 조회라 원자적이지 않으며 샘플 사이 순간 peak를 놓칠 수 있다. 예약 역전은 앞 offset보다 뒤 offset의 next-attempt-at이 이른 인접 쌍의 수다.

## HOL과 결과 해석

- A에서 Exponential은 공통 8초 Retry 총량을 줄였지만 peak는 Fixed와 같은 60이었다. 같은 횟수의 메시지들이 다시 비슷한 시간에 실행됐다.
- A Jitter는 peak 44였지만 p95 overdue가 약 5.7초다. 196개 attempt에서, 이미 발행됐고 실행 예정 시각도 지난 record가 앞 offset의 미래 시각 대기에 막힌 구간을 확인했다. 순수한 예약 분산 효과와 FIFO에 의한 처리 지연을 분리해야 한다.
- B Jitter의 전체 peak 39는 Exponential 35보다 높았다. 최초 8초 공통 구간 peak는 둘 다 15다. 총량 96 대 113만 보고 peak 개선까지 주장할 수 없다.
- B Fixed의 전체 peak는 가장 낮지만 32건이 DLQ다. 낮은 peak가 성공 복구의 우수성을 뜻하지 않는다.
- Fixed의 HOL 계수 0은 미래 Header 대기의 기여 하한이 0이라는 뜻이다. DB 연결, 재발행, commit 등 앞 작업을 기다리는 직렬 처리 비용까지 없다는 의미가 아니다.
- B의 최초 실패 수가 58 / 55 / 54로 달라 실제 재기동 시각과 처리 지연의 영향이 남아 있다. 전체 복구 시간 차이를 전략의 인과 효과나 통계적 개선율로 주장하지 않는다.

## 필수 검증 판정

| # | 검증 | 판정 | 근거 |
| --- | --- | --- | --- |
| 1 | Fixed 기존 동작 | PASS | Phase 3 8개 시나리오 회귀, 기본 2초/3회 유지 |
| 2 | Exponential 계산 | PASS | 단위 테스트 count 1~100, cap 포화·overflow·base>cap 검사 |
| 3 | Full Jitter 범위 | PASS | 단위 범위/seed 검사 및 실제 발행 난수열 재계산 |
| 4 | count별 Header | PASS | 모든 Retry raw record의 7개 Header, key/value, 원본 좌표·first-failed-at 대조 |
| 5 | 예약 이전 처리 없음 | PASS | 모든 실제 attempt 시작과 next-attempt-at 대조; 재구성 policy가 재추첨하지 않는 단위 검사 |
| 6 | 같은 실험 조건 | PASS | 시나리오별 모든 계획 설정 동등, 최초 8초 공통 중단 구간 검증; 전체 실제 장애 길이는 별도 실측·한계 명시 |
| 7 | Retry RPS 시계열 | PASS | 원시 시작 시각에서 1초 bin 생성, 0 구간 포함 |
| 8 | peak RPS | PASS | 시계열 최대값, 전체/공통 장애 구간 분리 |
| 9 | 전체 횟수 | PASS | actual attempt 좌표와 Retry Kafka record를 1:1 대조, count별 집계 |
| 10 | scheduling delay | PASS | 시작-예약 시각, 평균/p95/max, HOL 하한 구간 계산 |
| 11 | Consumer lag | PASS | 실제 broker end/committed 시계열, SQL/종료 상태와 대조 |
| 12 | 성공/DLQ/미완료 | PASS | eventId 집합, DLQ 원본, product별 SQL 재고, broker offset 교차 확인 |
| 13 | 복구 시간 | PASS | 처리 종료/전량 성공 복구 분리, Fixed 미달성을 null로 기록 |
| 14 | Phase 0~3 회귀 | PASS | Phase 0 전체, Phase 1 정상 flow, Phase 2 A-D, Phase 3 8개 시나리오 |
| 15 | Phase 5 없음 | PASS | Store transaction·이벤트·schema 유지, Idempotency/Replay/scheduler 추가 없음 |

실행하지 않은 영역: 반복 seed의 통계적 검증, 다중 Worker/partition, 운영 규모 throughput, HA, Replay, scheduler 대체 효과는 **UNVERIFIED**다. 필수 검증 15개와 별개의 범위다.

## 실행 명령과 회귀 증거

실제로 Build, Regression, Scenarios, Audit을 각각 호출했다. 초기 실패 후 영향 있는 단계만 재실행했다.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase4.ps1 -Check Build
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase4.ps1 -Check Regression
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase4.ps1 -Check Scenarios
. ./scripts/env.ps1
go test '-tags=integration,phase2,phase3,phase4' -count=1 -timeout=3m -run '^TestPhase4Runs$/^A$/^fixed$' -v ./tests/integration
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase4.ps1 -Check Audit
```

Build에는 `gofmt`, `go test ./...`, `go vet ./...`, `go build ./...`, `go mod verify` 및 integration tag compile/vet이 포함된다. Regression은 기존 스크립트를 재사용한다. Go 1.26.5, Kafka 4.2.0, MySQL 8.4.8, kafka-go v0.4.51 및 MySQL driver v1.10.0을 유지했다.

Phase 2 C 회귀 topic은 `phase2.c.d75fe78b-7205-4915-accf-4f74c1748e7d`, partition 0/offset 0이다. Worker PID 32140→20088, 재고 100→98→96으로 같은 비즈니스 side effect의 중복이 여전히 재현됐다. 과거 evidence는 수정하지 않았고 이번 회귀는 Phase 4 evidence의 `Regression`에 포함했다.

## 검증 기록과 발생한 문제

검증 당시 raw evidence와 SHA-256을 기반으로 일곱 실행을 대조했다. 공개 저장소에는 Fixed, Exponential, Full Jitter의 A/B 결과와 비교 조건을 사람이 읽을 수 있는 이 문서로 유지한다. 제외한 첫 A Fixed는 실제 장애 24.498초로 비교 조건 FAIL이었으며 긴 기동의 근본 원인은 확정하지 않았다.

초기 회귀는 Phase 2 A의 초기 snapshot 이전에 FAIL했다. 최초 전체 오류 출력은 보존되지 않아 정확한 문구는 확인 불가다. 새 topic leader 준비 경계를 보완한 뒤 전체 회귀가 PASS했다. 초기 집계는 MySQL의 비JSON 진단 때문에 FAIL했으며, 드라이버 진단 보존·별도 집계 후 모든 비교 셀의 대조가 PASS했다. 제외 셀 재실행 및 회귀 evidence 생성 전의 중간 Audit 실패도 최종 PASS로 소급하지 않는다.

## 변경 범위

지연 계산/단위 테스트, Worker 옵션/시각 로그, Phase 4 실행·집계 테스트/스크립트, Phase 2 준비 대기, Phase 3 evidence 경로, 이 문서와 상세 문서·README·gate·원시 evidence를 추가 또는 수정했다. `.gitattributes`는 원시 JSON의 LF를 고정해 checkout 후에도 SHA-256을 유지한다. 외부 dependency 추가 **없음**. `go.mod`, `go.sum`, Compose, schema, API, 이벤트 계약과 inventory transaction은 변경하지 않았다. Idempotency, Replay, Rate Limit, 범용 scheduler 및 Phase 5 이상의 기능은 구현하지 않았다.
