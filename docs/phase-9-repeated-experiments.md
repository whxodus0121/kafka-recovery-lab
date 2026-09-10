# Phase 9 — Repeated Experiment Validation

## 1. 목표

Phase 4 Retry 전략 6개 cell과 Phase 7 Replay/Recovery 전략 3개 cell을 각각 독립된 새 run으로 3회 실행한다. 단일 로컬 실행의 방향성이 반복되는 부분과 환경·seed 편차가 큰 metric을 분리한다.

## 2. 배경 개념

로컬 Docker 실험은 container 기동, Windows scheduler, Kafka write timing과 random seed의 영향을 받는다. n=3의 median/min/max는 관측 범위를 설명하지만 통계적 유의성, 신뢰구간이나 운영 SLA를 증명하지 않는다.

Phase 4의 전체 wall-clock은 MySQL 실제 outage 길이에 영향을 받는다. 따라서 첫 8초 공통 장애 구간의 Retry와 DB healthy 이후 recovery를 분리한다. Phase 7 peak RPS는 기존과 동일하게 1초 미만 sliding window 안의 최대 record 수다.

## 3. 왜 필요한가

기존 Phase 4와 Phase 7 결과는 전략별 한 번의 유효 실행이었다. 한 run의 peak나 완료 시간을 일반적인 전략 우위로 표현하면 환경 편차를 전략 효과로 오인할 수 있다. 이번 반복은 기존 결론을 맞추기 위한 재실행이 아니라 그 결론의 범위를 제한하기 위한 검증이다.

## 4. 시스템 구조

```text
Phase 4: 2 scenarios × 3 strategies × 3 seeds = 18 raw runs
  MySQL stop → first 8s common window → restore request → actual healthy → terminal state

Phase 7: 3 rate-control strategies × 3 repetitions = 9 raw runs
  DLQ 120 → Replay publication → Recovery Store → MySQL/offset reconciliation

raw JSON → unchanged Phase 4/7 audit definitions → median/min/max → Phase 9 evidence
```

기존 Worker, Retry 계산, Replay CLI, Recovery Store와 limiter를 그대로 사용했다. Phase 9 변경은 반복 번호, deterministic seed와 별도 raw 출력 경로를 harness에 전달하고 집계하는 범위다.

## 5. 구현 과정

Phase 4는 seed pair `(4101,4102)`, `(7301,7302)`, `(9201,9202)`를 repetition 1~3에 고정했다. Fixed와 Exponential도 같은 run 식별 체계를 사용했다. 모든 run은 60 events, 6 products, max Retry 5, base 1초, cap 8초, restore request 8초와 35초 관측을 유지했다.

Phase 7은 각 run마다 새 topic, group, event와 12개 product를 만들었다. Unlimited는 publication/recovery 0, Publication Limited는 publish 20/s, Backlog + Recovery Limited는 backlog를 먼저 120건 만든 뒤 recovery 20/s로 실행했다.

명백한 harness·인프라 실패만 invalid로 정의했다. 성능이나 DLQ 결과가 예상과 다른 것은 유효 run으로 유지했다. 실제 invalid run은 없었다.

## 6. 핵심 계약

- 모든 cell은 추가 선택 없이 정확히 3개의 valid run을 사용한다.
- 기존 raw evidence는 수정하거나 삭제하지 않고 `experiments/phase9`에 새 evidence를 저장한다.
- Phase 4 실제 outage를 8~12초 필터로 걸러내지 않는다.
- 첫 8초 Retry와 실제 DB healthy 이후 recovery를 별도로 집계한다.
- 낮은 peak는 success/DLQ/unfinished와 함께 해석한다.
- Phase 7은 `success + DLQ + unfinished = 120`, inventory, processed_events와 committed offset을 함께 검증한다.
- Prometheus/Grafana, exact lag collector와 새 metric은 추가하지 않는다.

## 7. 실행 및 검증

### Phase 4 Scenario A — 동시 burst

| Strategy | Runs | Total Retry median [min,max] | Common 8s Retry median [min,max] | Common/overall peak median [min,max] | Recovery after healthy s median [min,max] | Success / DLQ / unfinished |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Fixed | 3 | 300 [300,300] | 300 [300,300] | 60 [60,60] / 60 [60,60] | N/A | 0 / 60 / 0 |
| Exponential | 3 | 240 [240,240] | 180 [180,180] | 60 [60,60] / 60 [60,60] | 6.335 [6.238,6.346] | 60 / 0 / 0 |
| Full Jitter | 3 | 241 [230,242] | 172 [170,181] | 52 [40,54] / 52 [40,54] | 5.825 [4.689,6.141] | 60 / 0 / 0 |

Fixed의 낮지 않은 peak와 전체 DLQ를 함께 보면 부하 제어의 성공으로 해석할 수 없다. Exponential은 모든 run에서 같은 공통 구간 Retry 수를 보였고, Jitter는 seed에 따라 170~181회와 peak 40~54의 범위를 보였다.

### Phase 4 Scenario B — 지속 유입

| Strategy | Runs | Total Retry median [min,max] | Common 8s Retry median [min,max] | Common/overall peak median [min,max] | Recovery after healthy s median [min,max] | Success median [min,max] / DLQ median [min,max] |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Fixed | 3 | 195 [195,200] | 125 [125,125] | 25 [25,25] / 25 [25,25] | N/A | 36 [35,36] / 24 [24,25] |
| Exponential | 3 | 107 [106,114] | 37 [37,37] | 15 [15,15] / 29 [28,36] | 6.754 [5.395,6.860] | 60 [60,60] / 0 [0,0] |
| Full Jitter | 3 | 97 [88,124] | 38 [33,39] | 17 [15,18] / 42 [39,46] | 5.307 [3.223,6.137] | 60 [60,60] / 0 [0,0] |

Jitter의 전체 peak가 Exponential보다 낮다는 관계는 세 번 모두 성립하지 않았다. 반면 공통 장애 구간의 두 전략은 비슷한 범위였고 모두 60건을 성공시켰다.

### HOL과 overdue

Scenario A Jitter overdue p95는 5.149~5.956초, HOL attempt는 193~204회였다. Scenario B Jitter는 overdue 5.497~6.806초와 HOL 77~107회를 보였다. seed가 달라도 단일 Retry Worker가 미래 예약 record를 기다리며 뒤의 due record를 막는 현상이 반복됐다. Scenario B Exponential에서도 overdue 약 6.82초와 HOL 82~90회가 반복됐다.

Scenario B Fixed overdue p95는 기존 단일 run 2.089초와 달리 반복 run에서 14.785~15.512ms였다. 이 metric은 단일 결과로 일반화할 수 없다.

### Phase 7 반복 결과

| Strategy | Runs | Publish RPS median [min,max] | Processing RPS median [min,max] | Publish / processing peak median [min,max] | Peak lag median [min,max] | Business recovery ms median [min,max] |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Unlimited | 3 | 84.85 [81.57,86.21] | 84.88 [81.52,86.18] | 88 [85,90] / 88 [85,90] | 2 [1,4] | 2020 [1952,2177] |
| Publication Limited | 3 | 19.73 [19.70,19.77] | 19.72 [19.70,19.76] | 20 [20,20] / 20 [20,20] | 1 [1,1] | 6358 [6091,6513] |
| Backlog + Recovery Limited | 3 | 84.32 [82.59,84.47] | 19.75 [19.73,19.77] | 87 [85,87] / 20 [20,20] | 120 [120,120] | 8014 [7924,8132] |

모든 Phase 7 run은 success 120, DLQ 0, unfinished 0이며 inventory 12,000→11,880, processed_events 120, Recovery end/committed offset 120을 확인했다. Backlog 전략은 CLI 완료 median 1.906초와 business 완료 8.014초가 분리됐다.

## 8. 발생한 문제

의도적인 MySQL stop 동안 Go MySQL driver가 `unexpected EOF` 진단을 출력했다. Worker raw log와 별개인 예상된 연결 단절 신호이며 실험은 실제 healthy probe와 최종 reconciliation을 통과했다.

환경 또는 harness 오류로 분류할 invalid run은 없었다. 결과가 불리하다는 이유로 추가 run을 실행하지 않았다.

## 9. 결과와 한계

Phase 4에서 Fixed의 Retry 소진과 DLQ, Exponential/Jitter의 성공 경향은 반복됐다. Jitter의 HOL과 수초 단위 overdue도 세 seed에서 반복됐다. 그러나 Jitter peak와 recovery는 seed·scheduler에 따라 범위가 있었고, Scenario B Fixed overdue는 기존 단일 run과 크게 달랐다.

Phase 7에서는 publication 20/s와 backlog recovery 20/s가 각각 자신의 제어 지점에서 안정적인 평균·peak 범위를 보였다. Unlimited 처리량은 기존 단일 run 약 72~74/s보다 이번 반복에서 약 82~86/s로 높아 host 편차를 드러냈다. Publication Limited의 processing peak는 기존 23에서 세 번 모두 20이었지만, producer limiter가 DB 처리 rate를 보장한다는 구조적 결론으로 확대하지 않는다.

모든 결과는 Windows 로컬 Docker, 단일 Kafka broker, 단일 partition/Worker와 n=3에 한정된다. 통계적 유의성, 운영 규모, production SLA와 다른 host에서의 재현성은 확인하지 않았다.

## 10. 배운 점

반복해서 유지된 것은 특정 millisecond 값이 아니라 전략의 실패 형태와 제어 지점이었다. Fixed는 짧은 간격으로 Retry를 소진했고, backlog에 대한 Recovery limiter는 publication 완료와 business 완료를 분리하면서 Store 진입을 약 20/s로 제한했다.

peak 하나만으로 전략을 선택할 수 없다. Retry 수, DLQ, recovery, HOL과 실제 outage를 함께 봐야 한다. 또한 deterministic seed는 Jitter 계산을 재현하지만 OS scheduling과 container recovery까지 고정하지 않는다.

## 11. 다음 Phase 연결

Phase 9는 실험 근거의 범위를 보강하는 단계로 끝낸다. Phase 10의 포트폴리오 최종 재작성이나 새 Retry·scheduler·observability 기능은 구현하지 않았다.
