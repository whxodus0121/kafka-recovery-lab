param([ValidateSet('Build','Regression','Scenarios','Audit','All')][string]$Check='All')
$ErrorActionPreference='Stop'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    . ./scripts/env.ps1
    $checks=if($Check -eq 'All'){@('Build','Regression','Scenarios','Audit')}else{@($Check)}
    foreach($item in $checks) {
        switch($item) {
            'Build' {
                & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase3.ps1 -Check Build
                if($LASTEXITCODE -ne 0){throw 'Baseline build failed'}
                & go test '-tags=integration,phase2,phase3,phase4' -run '^$' ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Compile failed'}
                & go vet '-tags=integration,phase2,phase3,phase4' ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Vet failed'}
                Write-Output 'PHASE4_BUILD_PASS'
            }
            'Regression' {
                New-Item -ItemType Directory -Force bin | Out-Null
                $previous=$env:PHASE3_EVIDENCE_PATH
                try {
                    $env:PHASE3_EVIDENCE_PATH=Join-Path (Get-Location) 'bin/phase4-regression.json'
                    foreach($part in @('Regression','Topics','Scenarios')) {
                        & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase3.ps1 -Check $part
                        if($LASTEXITCODE -ne 0){throw "Regression $part failed"}
                    }
                } finally {$env:PHASE3_EVIDENCE_PATH=$previous}
                Write-Output 'PHASE4_REGRESSION_PASS'
            }
            'Scenarios' {
                & go test '-tags=integration,phase2,phase3,phase4' -count=1 -timeout=12m -run '^TestPhase4Runs$' -v ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 4 scenarios failed'}
                Write-Output 'PHASE4_SCENARIOS_PASS'
            }
            'Audit' {
                & go test '-tags=integration,phase2,phase3,phase4' -count=1 -timeout=2m -run '^TestPhase4Audit$' -v ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Evidence audit failed'}
                Write-Output 'PHASE4_AUDIT_PASS'
            }
        }
    }
} finally {Pop-Location}
