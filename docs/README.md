# Kafka Recovery Lab Documentation

이 폴더는 정상 기준선부터 장애 재현, Retry/DLQ, idempotency, Recovery, rate control과 반복 검증까지의 근거를 Phase 순서로 연결한다. 상세 문서와 verification 문서가 각 Phase의 설계와 검증 결과를 보존한다.

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

→ [프로젝트 README로 돌아가기](../README.md)
