# Phase 9 검증 결과

실행일: 2026-09-10 KST. 완료 조건은 **PASS 22 / FAIL 0 / UNVERIFIED 0**이다.

## 실행 결과

- Phase 4: Scenario A/B × Fixed/Exponential/Full Jitter × 3회 = 18 valid run
- Phase 7: Unlimited/Publication Limited/Backlog + Recovery Limited × 3회 = 9 valid run
- invalid run: 0
- deterministic seed pair: `(4101,4102)`, `(7301,7302)`, `(9201,9202)`
- 실제 Phase 4 outage: 9.701~11.166초, 결과 필터링 없음
- Phase 7의 9회 모두 success 120 / DLQ 0 / unfinished 0

상세 median/min/max와 기존 단일 run 비교는 [phase-9-repeated-experiments.md](phase-9-repeated-experiments.md)에 정리했다. 검증 당시 각 raw 경로와 SHA-256을 대조했다.

## 완료 조건

| # | 조건 | 판정 | 근거 |
| --- | --- | --- | --- |
| 1 | Build/Test | PASS | gofmt, test, vet, build, mod verify, Phase 9 tag compile/vet |
| 2 | Phase 4 A Fixed 3회 | PASS | raw 3개, DLQ 60/60/60 |
| 3 | Phase 4 A Exponential 3회 | PASS | raw 3개, success 60/60/60 |
| 4 | Phase 4 A Jitter 3회 | PASS | raw 3개, seed pair 3종 |
| 5 | Phase 4 B Fixed 3회 | PASS | raw 3개, success 35~36, DLQ 24~25 |
| 6 | Phase 4 B Exponential 3회 | PASS | raw 3개, success 60/60/60 |
| 7 | Phase 4 B Jitter 3회 | PASS | raw 3개, success 60/60/60 |
| 8 | 실제 outage 기록 | PASS | 18개 raw에 start/request/healthy와 9.701~11.166초 기록 |
| 9 | 공통 8초 집계 | PASS | attempts와 peak를 cell별 median/min/max 집계 |
| 10 | DB healthy 이후 recovery | PASS | 성공 완료 cell 집계, DLQ cell은 N/A |
| 11 | Retry/peak/overdue/HOL | PASS | 기존 정의로 18개 raw 재감사 |
| 12 | Phase 4 terminal reconciliation | PASS | success/DLQ/unfinished와 SQL side effect 일치 |
| 13 | Phase 7 Unlimited 3회 | PASS | 120건씩 실제 Kafka/MySQL 처리 |
| 14 | Phase 7 Publication Limited 3회 | PASS | publish average 19.70~19.77/s |
| 15 | Phase 7 Recovery Limited 3회 | PASS | backlog 120, processing average 19.73~19.77/s |
| 16 | publication/processing/lag 집계 | PASS | 기존 sliding peak와 broker offset 측정 |
| 17 | CLI/business 완료 분리 | PASS | backlog CLI 1.822~2.045초, business 7.924~8.132초 |
| 18 | Phase 7 reconciliation | PASS | 모든 run 120+0+0=120, inventory/marker/offset 일치 |
| 19 | raw와 SHA-256 | PASS | Phase 4 18개, Phase 7 9개, environment 1개 연결 |
| 20 | median/min/max와 기존 결과 비교 | PASS | evidence와 상세 문서 기록 |
| 21 | 문서/evidence/gates | PASS | Phase 9 결과물과 README 링크 |
| 22 | 범위 외 기능 없음 | PASS | 새 전략, scheduler, metric, dashboard 변경 없음 |

## 실행 명령

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase9.ps1 -Check Environment
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase9.ps1 -Check Phase4
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase9.ps1 -Check Phase7
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase9.ps1 -Check Audit
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase9.ps1 -Check Build
```

Phase 0~8 integration, Phase 5/6 전체 기능 시나리오와 Phase 8 Prometheus/Grafana 시나리오는 재실행하지 않았다. 기존 Phase 4/7 raw도 변경하지 않았다.

## 판정 한계

n=3의 로컬 관측이므로 통계적 유의성, 신뢰구간, production SLA를 주장하지 않는다. Fixed cell은 DLQ가 있어 recovery duration을 계산하지 않았으며 이를 `UNVERIFIED`가 아니라 정의상 N/A로 기록했다. 요청된 항목 중 실행하지 않은 것은 없다.
