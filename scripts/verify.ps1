param(
    [ValidateSet('Build', 'Infrastructure', 'Topics', 'Kafka', 'MySQL', 'Persistence', 'All')]
    [string]$Check = 'All'
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent

function Invoke-Checked {
    param([string]$Program, [string[]]$Arguments)
    & $Program @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$Program failed (exit $LASTEXITCODE)" }
}

function Get-MySQLState {
    $id = & docker compose ps -q mysql
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($id)) { throw 'MySQL container missing' }
    $json = & docker inspect --format '{{json .State}}' $id
    if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect MySQL container state' }
    return ($json | ConvertFrom-Json)
}

function Confirm-Topic {
    param([string]$Topic, [int]$Partitions)
    # Run twice: existing topics are allowed, but incompatible settings fail
    # verification below instead of being silently changed.
    for ($attempt = 0; $attempt -lt 2; $attempt++) {
        Invoke-Checked docker @('compose', 'exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'kafka:19092', '--create', '--if-not-exists', '--topic', $Topic, '--partitions', "$Partitions", '--replication-factor', '1', '--config', 'cleanup.policy=delete', '--config', 'retention.ms=604800000')
    }
    $description = & docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic $Topic
    if ($LASTEXITCODE -ne 0) { throw "Topic describe failed: $Topic" }
    $description | Write-Output
    $header = $description -join "`n"
    if ($header -notmatch "PartitionCount:\s+$Partitions\b" -or $header -notmatch 'ReplicationFactor:\s+1\b' -or $header -notmatch 'cleanup.policy=delete' -or $header -notmatch 'retention.ms=604800000') {
        throw "Topic configuration mismatch: $Topic"
    }
    $partitionLines = @($description | Where-Object { $_ -match 'Partition:\s+\d+' })
    if ($partitionLines.Count -ne $Partitions) { throw "Wrong partition count: $Topic" }
    foreach ($line in $partitionLines) {
        if ($line -notmatch 'Leader:\s+1\s+Replicas:\s+1\s+Isr:\s+1\b') {
            throw "Partition is not ready: $Topic"
        }
    }
}

Push-Location $root
try {
    $checks = if ($Check -eq 'All') { @('Build', 'Infrastructure', 'Topics', 'Kafka', 'MySQL', 'Persistence') } else { @($Check) }
    if ($Check -ne 'Build') { . (Join-Path $PSScriptRoot 'env.ps1') }
    foreach ($item in $checks) {
        switch ($item) {
            'Build' {
                $unformatted = & gofmt -l cmd internal
                if ($LASTEXITCODE -ne 0 -or $unformatted) { throw 'gofmt verification failed' }
                Invoke-Checked go @('mod', 'verify')
                Invoke-Checked go @('test', './...')
                Invoke-Checked go @('vet', './...')
                Invoke-Checked go @('build', './...')
                Write-Output 'BUILD_PASS'
            }
            'Infrastructure' {
                Invoke-Checked docker @('compose', 'config', '--quiet')
                $services = & docker compose config --services
                if ($LASTEXITCODE -ne 0 -or (($services | Sort-Object) -join ',') -ne 'kafka,mysql') { throw 'Unexpected Compose services' }
                Invoke-Checked docker @('compose', 'up', '-d', '--wait', '--wait-timeout', '180')
                $containerIDs = @(& docker compose ps -q)
                if ($LASTEXITCODE -ne 0 -or $containerIDs.Count -ne 2) { throw 'Expected exactly two containers' }
                # Inspect only required fields. Avoid parsing Compose's complete
                # JSON status (including host paths) through Windows code pages.
                foreach ($service in $services) {
                    $containerID = & docker compose ps -q $service
                    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($containerID)) { throw "Missing service: $service" }
                    $state = & docker inspect --format '{{.State.Status}} {{.State.Health.Status}}' $containerID
                    if ($LASTEXITCODE -ne 0 -or $state -ne 'running healthy') { throw "Unhealthy service: $service" }
                    $image = & docker inspect --format '{{.Config.Image}}' $containerID
                    if ($LASTEXITCODE -ne 0) { throw "Image inspection failed: $service" }
                    Write-Output "${service}: $state image=$image"
                }
                Invoke-Checked docker @('compose', 'exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh', '--version')
                Invoke-Checked docker @('compose', 'exec', '-T', 'mysql', 'mysqld', '--version')
                Write-Output 'INFRASTRUCTURE_PASS'
            }
            'Topics' {
                Confirm-Topic 'orders.created.v1' 6
                Confirm-Topic 'phase0.smoke.v1' 1
                Write-Output 'TOPICS_PASS'
            }
            'Kafka' { Invoke-Checked go @('run', './cmd/smoke', 'kafka') }
            'MySQL' { Invoke-Checked go @('run', './cmd/smoke', 'mysql') }
            'Persistence' {
                $probeID = [Guid]::NewGuid().ToString('N')
                Invoke-Checked go @('run', './cmd/smoke', 'mysql-write', $probeID)
                Invoke-Checked go @('run', './cmd/smoke', 'mysql-read', $probeID)
                $before = Get-MySQLState
                Invoke-Checked docker @('compose', 'restart', 'mysql')
                $deadline = [DateTime]::UtcNow.AddSeconds(180)
                do {
                    $after = Get-MySQLState
                    if ($after.Status -eq 'running' -and $after.Health.Status -eq 'healthy' -and $after.StartedAt -ne $before.StartedAt) { break }
                    if ([DateTime]::UtcNow -ge $deadline) { throw 'MySQL restart/health timeout; probe row retained for diagnosis' }
                    Start-Sleep -Seconds 2
                } while ($true)
                Write-Output "MySQL restarted: before=$($before.StartedAt) after=$($after.StartedAt)"
                Invoke-Checked go @('run', './cmd/smoke', 'mysql-read', $probeID)
                Invoke-Checked go @('run', './cmd/smoke', 'mysql-clean', $probeID)
                Write-Output "PERSISTENCE_PASS id=$probeID"
            }
        }
    }
} finally { Pop-Location }
