param([ValidateSet('Build','Regression','A','B','C','D','All')][string]$Check = 'All')
$ErrorActionPreference = 'Stop'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    . ./scripts/env.ps1
    $checks = if ($Check -eq 'All') { @('Build','Regression','A','B','C','D') } else { @($Check) }
    foreach ($item in $checks) {
        if ($item -eq 'Build') {
            & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase1.ps1 -Check Build
            if ($LASTEXITCODE -ne 0) { throw 'Baseline build failed' }
            & go vet '-tags=integration,phase2' ./tests/integration
            if ($LASTEXITCODE -ne 0) { throw 'Phase 2 vet failed' }
            Write-Output 'PHASE2_BUILD_PASS'
        } elseif ($item -eq 'Regression') {
            & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase1.ps1 -Check Phase0
            if ($LASTEXITCODE -ne 0) { throw 'Phase 0 failed' }
            New-Item -ItemType Directory -Force bin | Out-Null
            $previous = $env:PHASE1_EVIDENCE_PATH
            try {
                $env:PHASE1_EVIDENCE_PATH = Join-Path (Get-Location) 'bin/phase2-regression.json'
                & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-phase1.ps1 -Check Flow
                if ($LASTEXITCODE -ne 0) { throw 'Phase 1 failed' }
            } finally { $env:PHASE1_EVIDENCE_PATH = $previous }
            Write-Output 'PHASE2_REGRESSION_PASS'
        } else {
            & go test '-tags=integration,phase2' -count=1 -timeout=5m -v -run "^TestPhase2$item`$" ./tests/integration
            if ($LASTEXITCODE -ne 0) { throw "Scenario $item failed" }
            Write-Output "PHASE2_$($item)_PASS"
        }
    }
} finally { Pop-Location }
