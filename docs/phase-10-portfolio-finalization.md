# Phase 10 — GitHub / Portfolio Finalization

## 1. 목표

처음 저장소를 보는 사람이 프로젝트의 출발점, Kafka/MySQL 장애 경계, 설계 판단, 대표 실험 결과, Phase별 근거와 한계를 README에서 3~5분 안에 파악하도록 문서 탐색 구조를 정리한다. 기능 코드와 기존 실험 결과는 변경하지 않는다.

## 2. README 최종 구조

README 상단을 프로젝트 소개, Why This Project, Core Goal, Architecture, Tech Stack과 Key Results 순서로 재구성했다. 이후 Development Journey, 실제 repository 구조, 최소 정상 실행, 검증 방식과 한계를 배치했다.

대표 결과는 DB commit 이후 crash 전후의 `100 → 98 → 96`과 `100 → 98 → 98`, Phase 9 Retry 반복 결과, backlog Recovery 20/s 제어와 Phase 8 관측 검증으로 제한했다. Kafka와 MySQL exactly-once를 주장하지 않는다.

## 3. docs navigation

[Documentation Index](README.md)는 Phase 0~10의 주제, 실제 상세 문서와 verification을 한 표에서 연결한다. 개별 verification에서 evidence JSON과 raw experiment로 이동할 수 있으며, Phase 10은 실험 단계가 아니므로 evidence JSON을 추가하지 않았다.

## 4. GitHub link audit

README와 `docs/*.md`의 상대 Markdown 링크를 각 파일 위치에서 해석해 존재 여부를 검사한다. heading level, code fence, Mermaid block 시작과 Markdown table separator도 구조적으로 검사한다.

과거 verification과 gate에 기록된 로컬 작업 경로는 수치나 실행 결과를 바꾸지 않고 `project root`로 치환했다. GitHub에서 열 수 없는 Windows absolute path와 로컬 파일 URI를 남기지 않는다.

## 5. repository hygiene

tracked/untracked 후보에서 `.env`, credential signature, private key, editor backup, OS metadata와 실행 binary를 검사한다. 실제 `.env`는 `.gitignore` 대상이고 `.env.example`에는 빈 password placeholder만 남긴다.

Phase 4/7/9 raw JSON, verification/evidence와 Prometheus/Grafana provisioning은 삭제하거나 수정하지 않았다. Git LFS와 새 dependency도 추가하지 않았다.

## 6. build verification

성능 및 integration scenario를 재실행하지 않고 다음 정적·단위 검증만 수행한다.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase10.ps1 -Check All
```

이 명령은 gofmt 변경 필요 여부, `go test ./...`, `go vet ./...`, `go build ./...`, `go mod verify`, Markdown navigation과 repository hygiene를 검사한다.

최종 실행에서 Build, Markdown과 Hygiene 세 check가 모두 PASS했다. Phase 4/7/9 성능 실험과 Phase 8 integration은 실행하지 않았다.

## 7. 변경된 문서 목록

- `README.md`
- `docs/README.md`
- `docs/phase-10-portfolio-finalization.md`
- `PHASE10_GATES.md`
- 과거 Markdown의 로컬 absolute path 표기
- `scripts/verify-phase10.ps1`

## 8. 유지한 evidence

Phase 0~9 verification/evidence 수치와 `experiments/` raw JSON은 재계산, 삭제 또는 수정하지 않았다. Phase 4/7/9 성능 실험과 Phase 8 Prometheus/Grafana integration도 재실행하지 않았다.

## 9. 결과와 한계

문서 구조와 재현 명령은 현재 단일 broker, 단일 partition/Worker 중심 구현을 그대로 설명한다. persistent Replay checkpoint, destination publish와 source commit의 원자성, Retry FIFO HOL, 전체 Retry lineage quota와 exact committed lag는 해결된 것처럼 표현하지 않는다. Replay CLI와 Recovery Worker의 기본 recovery topic 불일치는 코드 동결 범위에서 변경하지 않고 실행 옵션과 README 한계로 공개했다.

Markdown의 Mermaid 검사는 dependency를 추가하지 않는 구조 검사다. GitHub renderer 자체의 pixel-level 출력이나 외부 링크의 장기 가용성을 보장하지 않는다.

## 10. 최종 Git 상태

문서·정적 검증은 PASS했고 로컬 `main`에 `docs: finalize kafka recovery portfolio` commit을 만든다. Push 전 확인에서 현재 저장소에는 `origin` remote가 없었다. 따라서 fetch, fast-forward ancestry 확인, push와 remote hash 비교는 remote 설정을 임의 변경하지 않는 안전 조건에 따라 수행할 수 없다. 로컬 commit과 clean working tree는 유지하고 정확한 commit hash는 완료 보고에 기록한다.
