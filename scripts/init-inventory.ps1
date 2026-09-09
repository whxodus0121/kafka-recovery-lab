param([switch]$Seed)
$ErrorActionPreference = 'Stop'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    . ./scripts/env.ps1
    $sql = Get-Content -LiteralPath migrations/001_inventory.sql -Raw
    $sql += "`n" + (Get-Content -LiteralPath migrations/002_processed_events.sql -Raw)
    if ($Seed) { $sql += "`n" + (Get-Content -LiteralPath scripts/seed-inventory.sql -Raw) }
    $mysqlCommand = 'MYSQL_PWD="$MYSQL_PASSWORD" mysql --protocol=TCP --host=127.0.0.1 --user="$MYSQL_USER" --database="$MYSQL_DATABASE" --batch'
    $sql | docker compose exec -T mysql sh -c $mysqlCommand
    if ($LASTEXITCODE -ne 0) { throw 'Inventory schema/seed SQL failed' }
    Write-Output 'INVENTORY_INITIALIZED'
} finally { Pop-Location }
