param([ValidateSet('Environment','Phase4','Phase7','Audit','Build','All')][string]$Check='All')
$ErrorActionPreference='Stop'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    . ./scripts/env.ps1
    $tags='-tags=integration,phase2,phase3,phase4,phase5,phase6,phase7,phase8,phase9'
    # A completed checkout already contains the immutable raw set. In that state,
    # All verifies the preserved evidence instead of creating extra repetitions.
    $preservedEvidence=$Check -eq 'All' -and (Test-Path 'docs/phase-9-evidence.json')
    $checks=if($preservedEvidence){@('Audit','Build')}elseif($Check -eq 'All'){@('Environment','Phase4','Phase7','Audit','Build')}else{@($Check)}
    foreach($item in $checks){
        switch($item){
            'Environment' {
                & docker compose up -d --wait kafka mysql
                if($LASTEXITCODE -ne 0){throw 'Kafka/MySQL environment failed'}
                & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/init-inventory.ps1
                if($LASTEXITCODE -ne 0){throw 'Schema initialization failed'}
                & go test $tags -count=1 -run '^TestPhase9Environment$' -v ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 9 environment capture failed'}
                Write-Output 'PHASE9_ENVIRONMENT_PASS'
            }
            'Phase4' {
                & go test $tags -count=1 -timeout=30m -run '^TestPhase9Phase4$' -v ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 4 repeated experiments failed; preserve raw failures and replace only invalid runs'}
                Write-Output 'PHASE9_PHASE4_PASS'
            }
            'Phase7' {
                & go test $tags -count=1 -timeout=15m -run '^TestPhase9Phase7$' -v ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 7 repeated experiments failed; preserve raw failures and replace only invalid runs'}
                Write-Output 'PHASE9_PHASE7_PASS'
            }
            'Audit' {
                if($preservedEvidence){$env:PHASE9_AUDIT_ONLY='1'}
                & go test $tags -count=1 -run '^TestPhase9Audit$' -v ./tests/integration
                Remove-Item Env:PHASE9_AUDIT_ONLY -ErrorAction SilentlyContinue
                if($LASTEXITCODE -ne 0){throw 'Phase 9 evidence audit failed'}
                Write-Output 'PHASE9_AUDIT_PASS'
            }
            'Build' {
                $unformatted=& gofmt -l cmd internal tests
                if($LASTEXITCODE -ne 0 -or $unformatted){throw 'gofmt failed'}
                foreach($arguments in @(@('test','./...'),@('vet','./...'),@('build','./...'),@('mod','verify'))){
                    & go @arguments
                    if($LASTEXITCODE -ne 0){throw 'Go verification failed'}
                }
                & go test $tags -count=1 -run '^TestPhase9Summarize$' ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 9 integration compile or aggregate helper test failed'}
                & go vet $tags ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 9 integration vet failed'}
                Write-Output 'PHASE9_BUILD_PASS'
            }
        }
    }
} finally {Pop-Location}
