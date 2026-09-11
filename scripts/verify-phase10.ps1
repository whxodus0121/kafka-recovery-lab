param([ValidateSet('Build','Markdown','Hygiene','All')][string]$Check='All')
$ErrorActionPreference='Stop'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    $checks=if($Check -eq 'All'){@('Build','Markdown','Hygiene')}else{@($Check)}
    foreach($item in $checks){
        switch($item){
            'Build' {
                $unformatted=& gofmt -l cmd internal tests
                if($LASTEXITCODE -ne 0 -or $unformatted){throw "gofmt failed: $unformatted"}
                foreach($arguments in @(@('test','./...'),@('vet','./...'),@('build','./...'),@('mod','verify'))){
                    & go @arguments
                    if($LASTEXITCODE -ne 0){throw "Go verification failed: go $($arguments -join ' ')"}
                }
                Write-Output 'PHASE10_BUILD_PASS'
            }
            'Markdown' {
                $markdown=@(Get-Item README.md)+@(Get-ChildItem docs -File -Filter *.md)
                foreach($file in $markdown){
                    $content=[IO.File]::ReadAllText($file.FullName)
                    if($content -match '(?i)(^|[\s`"''(])(?:[a-z]:[\\/]|file://)'){throw "Local path in $($file.FullName)"}
                    $lines=[IO.File]::ReadAllLines($file.FullName)
                    $fences=@($lines|Where-Object{$_ -match '^\s*```'}).Count
                    if($fences % 2 -ne 0){throw "Unbalanced code fence in $($file.FullName)"}
                    $previous=0
                    foreach($line in $lines){
                        if($line -match '^(#+)\s'){
                            $level=$Matches[1].Length
                            if($previous -gt 0 -and $level -gt $previous+1){throw "Heading level skipped in $($file.FullName)"}
                            $previous=$level
                        }
                    }
                    foreach($match in [regex]::Matches($content,'(?m)!?\[[^\]]*\]\((?<target>[^)]+)\)')){
                        $target=$match.Groups['target'].Value.Trim().Trim('<','>')
                        if($target -match '^(?i:https?://|mailto:|#)'){continue}
                        $pathPart=($target -split '#',2)[0]
                        if([string]::IsNullOrWhiteSpace($pathPart)){continue}
                        $resolved=Join-Path $file.DirectoryName ([Uri]::UnescapeDataString($pathPart).Replace('/',[IO.Path]::DirectorySeparatorChar))
                        if(-not(Test-Path -LiteralPath $resolved)){throw "Broken link in $($file.FullName): $target"}
                    }
                    foreach($match in [regex]::Matches($content,'(?ms)^```mermaid\s*\r?\n(?<body>.*?)^```\s*$')){
                        if($match.Groups['body'].Value -notmatch '^\s*((flowchart|graph)\s+(TD|TB|BT|RL|LR)\b|sequenceDiagram\b)'){throw "Unsupported Mermaid start in $($file.FullName)"}
                    }
                    for($i=1;$i -lt $lines.Count;$i++){
                        if($lines[$i] -match '^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?\s*$' -and $lines[$i-1] -notmatch '\|'){throw "Table separator without header in $($file.FullName)"}
                    }
                }
                for($phase=0;$phase -le 9;$phase++){
                    if((Get-Content docs/README.md -Raw) -notmatch "\| $phase \|"){throw "docs index missing Phase $phase"}
                }
                Write-Output 'PHASE10_MARKDOWN_PASS'
            }
            'Hygiene' {
                $files=@(git ls-files --cached --others --exclude-standard)
                if($LASTEXITCODE -ne 0){throw 'git file inventory failed'}
                $forbidden=$files|Where-Object{$_ -match '(^|/)(\.env|\.DS_Store|Thumbs\.db)$|(~|\.bak|\.orig|\.swp|\.tmp)$|\.(exe|dll|so|dylib|test|out)$'}
                if($forbidden){throw "Forbidden repository files: $($forbidden -join ', ')"}
                $trackedEnv=@(git ls-files -- .env)
                if($trackedEnv.Count -gt 0){throw '.env is tracked'}
                $secretPattern='BEGIN (RSA|OPENSSH|EC|DSA) PRIVATE KEY|AKIA[0-9A-Z]{16}|ghp_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,}'
                $secretHits=& rg -l -I -e $secretPattern --glob '!experiments/**' --glob '!docs/*evidence.json' . 2>$null
                if($LASTEXITCODE -eq 0 -and $secretHits){throw "Secret signature found: $($secretHits -join ', ')"}
                if((git diff --name-only -- go.mod go.sum)){throw 'Dependency files changed'}
                Write-Output 'PHASE10_HYGIENE_PASS'
            }
        }
    }
} finally {Pop-Location}
