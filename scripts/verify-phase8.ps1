param([ValidateSet('Build','Infrastructure','Scenarios','Regression','Audit','All')][string]$Check='All')
$ErrorActionPreference='Stop'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    . ./scripts/env.ps1
    $tags='-tags=integration,phase2,phase3,phase4,phase5,phase6,phase7,phase8'
    $checks=if($Check -eq 'All'){@('Build','Infrastructure','Scenarios','Regression','Audit')}else{@($Check)}
    foreach($item in $checks){
        switch($item){
            'Build' {
                $unformatted=& gofmt -l cmd internal tests
                if($LASTEXITCODE -ne 0 -or $unformatted){throw 'gofmt failed'}
                foreach($arguments in @(@('test','./...'),@('vet','./...'),@('build','./...'),@('mod','verify'))){
                    & go @arguments
                    if($LASTEXITCODE -ne 0){throw 'Go verification failed'}
                }
                & go test $tags -run '^$' ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 8 integration compile failed'}
                & go vet $tags ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 8 integration vet failed'}
                Write-Output 'PHASE8_BUILD_PASS'
            }
            'Infrastructure' {
                & docker compose config --quiet
                if($LASTEXITCODE -ne 0){throw 'Compose configuration invalid'}
                & docker run --rm -v "${PWD}/monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro" --entrypoint /bin/promtool prom/prometheus:v3.5.0 check config /etc/prometheus/prometheus.yml
                if($LASTEXITCODE -ne 0){throw 'Prometheus configuration invalid'}
                & docker compose up -d --wait kafka mysql prometheus grafana
                if($LASTEXITCODE -ne 0){throw 'Monitoring infrastructure unhealthy'}
                $prometheus=Invoke-RestMethod -Uri 'http://127.0.0.1:9090/-/healthy'
                $grafana=Invoke-RestMethod -Uri 'http://127.0.0.1:3000/api/health'
                $datasource=Invoke-RestMethod -Uri 'http://127.0.0.1:3000/api/datasources/uid/prometheus'
                $dashboard=Invoke-RestMethod -Uri 'http://127.0.0.1:3000/api/dashboards/uid/kafka-recovery-lab'
                if($prometheus.Trim() -ne 'Prometheus Server is Healthy.' -or $grafana.database -ne 'ok' -or $datasource.url -ne 'http://prometheus:9090' -or $dashboard.dashboard.panels.Count -ne 9){throw 'Monitoring provisioning mismatch'}
                Write-Output 'PHASE8_INFRASTRUCTURE_PASS'
            }
            'Scenarios' {
                & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/init-inventory.ps1
                if($LASTEXITCODE -ne 0){throw 'Schema initialization failed'}
                & go test $tags -count=1 -timeout=6m -run '^TestPhase8$' -v ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 8 metric scenarios failed'}
                Write-Output 'PHASE8_SCENARIOS_PASS'
            }
            'Regression' {
                & go test ./internal/inventory ./internal/ratelimit ./internal/observability
                if($LASTEXITCODE -ne 0){throw 'Affected unit regression failed'}
                & go test $tags -count=1 -run '^TestPhase(6Evidence|8PriorEvidence)$' ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Preserved Phase 4 through 7 evidence regression failed'}
                Write-Output 'PHASE8_REGRESSION_PASS'
            }
            'Audit' {
                & go test $tags -count=1 -run '^TestPhase8Audit$' ./tests/integration
                if($LASTEXITCODE -ne 0){throw 'Phase 8 evidence audit failed'}
                Write-Output 'PHASE8_AUDIT_PASS'
            }
        }
    }
} finally {Pop-Location}
