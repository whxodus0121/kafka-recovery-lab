param([ValidateSet('Build','Topics','Scenarios','Regression','Audit','All')][string]$Check='All')
$ErrorActionPreference='Stop'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    . ./scripts/env.ps1
    $tags='-tags=integration,phase2,phase3,phase4,phase5,phase6,phase7'
    $checks=if($Check -eq 'All'){@('Build','Topics','Scenarios','Regression','Audit')}else{@($Check)}
    foreach($item in $checks){
        switch($item){
            'Build' {
                & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase6.ps1 -Check Build
                if($LASTEXITCODE -ne 0){throw 'Phase 0 through 6 build regression failed'}
                & go test $tags -run '^$' ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 7 integration compile failed'}
                & go vet $tags ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 7 integration vet failed'}
                & go test ./cmd/replay ./internal/ratelimit ./internal/inventory
                if($LASTEXITCODE -ne 0){throw 'Phase 7 affected unit tests failed'}
                Write-Output 'PHASE7_BUILD_PASS'
            }
            'Topics' {
                & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase6.ps1 -Check Topics
                if($LASTEXITCODE -ne 0){throw 'Phase 6 topic regression failed'}
                Write-Output 'PHASE7_TOPICS_PASS'
            }
            'Scenarios' {
                & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/init-inventory.ps1
                if($LASTEXITCODE -ne 0){throw 'Schema initialization failed'}
                & go test $tags -count=1 -timeout=12m -run '^TestPhase7$' -v ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 7 scenarios failed'}
                Write-Output 'PHASE7_SCENARIOS_PASS'
            }
            'Regression' {
                & go test $tags -count=1 -run '^TestPhase6(Evidence|NoUnexpectedDependencies)$' ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 5 and 6 preserved evidence regression failed'}
                Write-Output 'PHASE7_REGRESSION_PASS'
            }
            'Audit' {
                & go test $tags -count=1 -run '^TestPhase7Audit$' ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 7 evidence audit failed'}
                Write-Output 'PHASE7_AUDIT_PASS'
            }
        }
    }
} finally {Pop-Location}
