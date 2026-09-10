# Kafka Recovery Lab Documentation

이 폴더는 정상 기준선부터 장애 재현, Retry/DLQ, idempotency, Recovery, rate control과 반복 검증까지의 근거를 Phase 순서로 연결한다. 처음 읽는 경우 상세 문서와 verification을 같은 행에서 확인한 뒤, verification이 가리키는 evidence JSON과 raw experiment로 내려가는 순서를 권장한다.

| Phase | Topic | Detailed Document | Verification |
| ---: | --- | --- | --- |
| 0 | 재현 가능한 Kafka/MySQL 환경 | [Environment](phase-0-environment.md) | [Verification](phase-0-verification.md) |
| 1 | Order API부터 inventory 차감까지의 정상 흐름 | [Normal Flow](phase-1-normal-flow.md) | [Verification](phase-1-verification.md) |
| 2 | DB commit과 Kafka offset commit 장애 경계 | [Failure Boundaries](phase-2-failure-boundaries.md) | [Verification](phase-2-verification.md) |
| 3 | Error 분류, Fixed Retry와 DLQ | [Retry and DLQ](phase-3-retry-dlq.md) | [Verification](phase-3-verification.md) |
| 4 | Retry Storm, Exponential Backoff와 Full Jitter | [Backoff and Jitter](phase-4-backoff-jitter.md) | [Verification](phase-4-verification.md) |
| 5 | MySQL transactional idempotency | [Idempotency](phase-5-idempotency.md) | [Verification](phase-5-verification.md) |
| 6 | 단건 DLQ Replay와 실제 DB Recovery | [DLQ Recovery](phase-6-dlq-recovery.md) | [Verification](phase-6-verification.md) |
| 7 | Bulk Replay와 publication/recovery rate control | [Replay Rate Limit](phase-7-replay-rate-limit.md) | [Verification](phase-7-verification.md) |
| 8 | Prometheus/Grafana observability | [Observability](phase-8-observability.md) | [Verification](phase-8-verification.md) |
| 9 | Phase 4/7 핵심 실험의 3회 반복 검증 | [Repeated Experiments](phase-9-repeated-experiments.md) | [Verification](phase-9-verification.md) |
| 10 | GitHub와 포트폴리오 최종 정리 | [Portfolio Finalization](phase-10-portfolio-finalization.md) | - |

## Evidence Layout

- `phase-N-evidence.json`: verification에서 선택·집계한 구조화 evidence
- `../experiments/phaseN/`: 개별 실행의 immutable raw JSON
- `../PHASEN_GATES.md`: Phase별 완료 조건과 실제 실행 근거

Phase 0에는 별도 evidence JSON이 없으며 verification 문서가 실행 결과를 보존한다. Phase 10은 기능·성능 실험이 아니므로 대형 evidence JSON을 추가하지 않는다.

→ [프로젝트 README로 돌아가기](../README.md)
