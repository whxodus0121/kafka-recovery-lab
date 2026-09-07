param([ValidateSet('Build','Phase0','Flow','All')][string]$Check = 'All')
$ErrorActionPreference = 'Stop'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    . ./scripts/env.ps1
    $checks = if ($Check -eq 'All') { @('Build','Phase0','Flow') } else { @($Check) }
    foreach ($item in $checks) {
        switch ($item) {
            'Build' {
                $unformatted = & gofmt -l cmd internal tests
                if ($LASTEXITCODE -ne 0 -or $unformatted) { throw 'gofmt failed' }
                foreach ($arguments in @(@('test','./...'), @('vet','./...'), @('build','./...'), @('mod','verify'))) {
                    & go @arguments
                    if ($LASTEXITCODE -ne 0) { throw 'Go verification failed' }
                }
                git diff --exit-code -- go.mod go.sum
                if ($LASTEXITCODE -ne 0) { throw 'Unexpected dependency change' }
                & go test -tags=integration -run '^$' ./tests/integration
                if ($LASTEXITCODE -ne 0) { throw 'Integration test compile failed' }
                & go vet -tags=integration ./tests/integration
                if ($LASTEXITCODE -ne 0) { throw 'Integration vet failed' }
                Write-Output 'PHASE1_BUILD_PASS'
            }
            'Phase0' {
                & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify.ps1 -Check All
                if ($LASTEXITCODE -ne 0) { throw 'Phase 0 regression failed' }
                Write-Output 'PHASE0_REGRESSION_PASS'
            }
            'Flow' {
                # Integration test refuses a live group or pending records before
                # seeding. It launches and gracefully stops its own real processes.
                & go test -tags=integration -count=1 -timeout=4m -v ./tests/integration
                if ($LASTEXITCODE -ne 0) { throw 'Phase 1 flow failed' }
                Write-Output 'PHASE1_FLOW_PASS'
            }
        }
    }
} finally { Pop-Location }
