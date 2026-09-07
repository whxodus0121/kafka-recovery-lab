param([switch]$Init)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$envPath = Join-Path $root '.env'
if ($Init -and -not (Test-Path -LiteralPath $envPath)) {
    $content = [IO.File]::ReadAllText((Join-Path $root '.env.example'))
    $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        foreach ($key in @('MYSQL_PASSWORD', 'MYSQL_ROOT_PASSWORD')) {
            $bytes = New-Object byte[] 24
            $rng.GetBytes($bytes)
            $value = [BitConverter]::ToString($bytes).Replace('-', '').ToLowerInvariant()
            $content = [regex]::Replace($content, "(?m)^${key}=\r?$", "${key}=$value")
        }
        [IO.File]::WriteAllText($envPath, $content, (New-Object Text.UTF8Encoding($false)))
        Write-Host 'Created ignored .env with random local passwords (values not printed).'
    } finally { $rng.Dispose() }
}
if (-not (Test-Path -LiteralPath $envPath)) {
    throw 'Missing .env. Run: . ./scripts/env.ps1 -Init'
}
# Deliberately small dotenv subset. No evaluation, expansion, quotes or comments
# after values. Both Compose and Go receive the same literal values.
$allowed = @('KAFKA_BROKERS', 'KAFKA_PORT', 'KAFKA_ADVERTISED_HOST', 'MYSQL_HOST', 'MYSQL_PORT', 'MYSQL_DATABASE', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_ROOT_PASSWORD', 'SMOKE_TIMEOUT')
$optional = @('API_ADDR', 'KAFKA_CONSUMER_GROUP')
$seen = @{}
foreach ($line in [IO.File]::ReadAllLines($envPath)) {
    if ($line.Trim() -eq '' -or $line.TrimStart().StartsWith('#')) { continue }
    if ($line -notmatch '^([A-Z][A-Z0-9_]*)=([A-Za-z0-9_.,:/+\[\]-]*)$') {
        throw 'Unsupported .env syntax: use literal KEY=value without quotes, spaces or interpolation.'
    }
    $key, $value = $Matches[1], $Matches[2]
    if (($key -notin $allowed -and $key -notin $optional) -or $seen.ContainsKey($key)) { throw "Unknown or duplicate .env key: $key" }
    $seen[$key] = $true
    [Environment]::SetEnvironmentVariable($key, $value, 'Process')
}
foreach ($key in $allowed) {
    if (-not $seen.ContainsKey($key) -or [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($key, 'Process'))) {
        throw "Missing or empty .env key: $key"
    }
}
