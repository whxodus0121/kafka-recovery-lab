param([ValidateSet('Build','Regression','Topics','Scenarios','All')][string]$Check='All')
$ErrorActionPreference='Stop'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    . ./scripts/env.ps1
    $checks=if($Check -eq 'All'){@('Build','Regression','Topics','Scenarios')}else{@($Check)}
    foreach($item in $checks) {
        switch($item) {
            'Build' {
                & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase2.ps1 -Check Build
                if($LASTEXITCODE -ne 0){throw 'Baseline build failed'}
                & go test '-tags=integration,phase2,phase3' -run '^$' ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Compile failed'}
                & go vet '-tags=integration,phase2,phase3' ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Vet failed'}
                Write-Output 'PHASE3_BUILD_PASS'
            }
            'Regression' {
                New-Item -ItemType Directory -Force bin | Out-Null
                $previous=$env:PHASE2_EVIDENCE_PATH
                try {
                    $env:PHASE2_EVIDENCE_PATH=Join-Path (Get-Location) 'bin/phase3-regression.json'
                    & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase2.ps1 -Check Regression
                    if($LASTEXITCODE -ne 0){throw 'Phase 0/1 failed'}
                    foreach($scenario in @('A','B','C','D')) {
                        & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase2.ps1 -Check $scenario
                        if($LASTEXITCODE -ne 0){throw 'Phase 2 baseline failed'}
                    }
                } finally {$env:PHASE2_EVIDENCE_PATH=$previous}
                Write-Output 'PHASE3_REGRESSION_PASS'
            }
            'Topics' {
                foreach($topic in @('inventory.retry.v1','inventory.dlq.v1')) {
                    & docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic $topic --partitions 1 --replication-factor 1 --config cleanup.policy=delete --config retention.ms=604800000
                    if($LASTEXITCODE -ne 0){throw 'Topic creation failed'}
                    $description=& docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic $topic
                    if($LASTEXITCODE -ne 0 -or ($description -join ' ') -notmatch 'PartitionCount:\s+1\b' -or ($description -join ' ') -notmatch 'ReplicationFactor:\s+1\b'){throw 'Topic config mismatch'}
                }
                Write-Output 'PHASE3_TOPICS_PASS'
            }
            'Scenarios' {
                & go test '-tags=integration,phase2,phase3' -count=1 -timeout=8m -run '^TestPhase3' -v ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 3 scenarios failed'}
                Write-Output 'PHASE3_SCENARIOS_PASS'
            }
        }
    }
} finally {Pop-Location}
