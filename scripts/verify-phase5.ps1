param([ValidateSet('Build','Schema','Scenarios','Regression','All')][string]$Check='All')
$ErrorActionPreference='Stop'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    . ./scripts/env.ps1
    $tags='-tags=integration,phase2,phase3,phase4,phase5'
    $checks=if($Check -eq 'All'){@('Build','Scenarios','Regression')}else{@($Check)}
    foreach($item in $checks){
        switch($item){
            'Build' {
                & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase4.ps1 -Check Build
                if($LASTEXITCODE -ne 0){throw 'Baseline build failed'}
                & go test $tags -run '^$' ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Compile failed'}
                & go vet $tags ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Vet failed'}
                Write-Output 'PHASE5_BUILD_PASS'
            }
            {$_ -in 'Schema','Scenarios'} {
                & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/init-inventory.ps1
                if($LASTEXITCODE -ne 0){throw 'Schema failed'}
                if($item -eq 'Scenarios'){
                    & go test $tags -count=1 -timeout=6m -run '^TestPhase5$' -v ./tests/integration
                    if($LASTEXITCODE -ne 0){throw 'Phase 5 scenarios failed'}
                    Write-Output 'PHASE5_SCENARIOS_PASS'
                }
            }
            'Regression' {
                & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase4.ps1 -Check Build
                if($LASTEXITCODE -ne 0){throw 'Phase 4 strategy build regression failed'}
                & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase4.ps1 -Check Regression
                if($LASTEXITCODE -ne 0){throw 'Phase 0 through 3 regression failed'}
                & go test $tags -count=1 -run '^TestPhase5Evidence$' ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Evidence reconciliation failed'}
                Write-Output 'PHASE5_REGRESSION_PASS'
            }
        }
    }
} finally {Pop-Location}
