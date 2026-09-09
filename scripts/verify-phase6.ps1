param([ValidateSet('Build','Topics','Scenarios','Regression','All')][string]$Check='All')
$ErrorActionPreference='Stop'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    . ./scripts/env.ps1
    $tags='-tags=integration,phase2,phase3,phase4,phase5,phase6'
    $checks=if($Check -eq 'All'){@('Build','Topics','Scenarios','Regression')}else{@($Check)}
    foreach($item in $checks){
        switch($item){
            'Build' {
                & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase5.ps1 -Check Build
                if($LASTEXITCODE -ne 0){throw 'Phase 0 through 5 build regression failed'}
                & go test $tags -run '^$' ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 6 integration compile failed'}
                & go vet $tags ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 6 integration vet failed'}
                & go build ./cmd/replay
                if($LASTEXITCODE -ne 0){throw 'Replay CLI build failed'}
                Remove-Item -LiteralPath (Join-Path (Get-Location) 'replay.exe') -ErrorAction SilentlyContinue
                Write-Output 'PHASE6_BUILD_PASS'
            }
            'Topics' {
                & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase3.ps1 -Check Topics
                if($LASTEXITCODE -ne 0){throw 'Retry/DLQ topic regression failed'}
                $topic='inventory.recovery.v1'
                & docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic $topic --partitions 1 --replication-factor 1 --config cleanup.policy=delete --config retention.ms=604800000
                if($LASTEXITCODE -ne 0){throw 'Recovery topic creation failed'}
                $description=& docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic $topic
                if($LASTEXITCODE -ne 0 -or ($description -join ' ') -notmatch 'PartitionCount:\s+1\b' -or ($description -join ' ') -notmatch 'ReplicationFactor:\s+1\b'){throw 'Recovery topic config mismatch'}
                Write-Output 'PHASE6_TOPICS_PASS'
            }
            'Scenarios' {
                & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/init-inventory.ps1
                if($LASTEXITCODE -ne 0){throw 'Schema initialization failed'}
                & go test $tags -count=1 -timeout=12m -run '^TestPhase6$' -v ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 6 scenarios failed'}
                Write-Output 'PHASE6_SCENARIOS_PASS'
            }
            'Regression' {
                & go test ./internal/inventory
                if($LASTEXITCODE -ne 0){throw 'Retry/idempotency unit regression failed'}
                & go test $tags -count=1 -run '^TestPhase6(Evidence|NoUnexpectedDependencies)$' ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Preserved evidence regression failed'}
                Write-Output 'PHASE6_REGRESSION_PASS'
            }
        }
    }
} finally {Pop-Location}
