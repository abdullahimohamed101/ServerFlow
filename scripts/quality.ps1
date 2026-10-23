param(
    [ValidateSet("Fast", "Full")]
    [string]$Mode = "Fast"
)

$ErrorActionPreference = "Stop"

# ============================================================
# AI ENGINEERING QUALITY GATE
#
# Usage:
#
#   .\scripts\quality.ps1 -Mode Fast
#   .\scripts\quality.ps1 -Mode Full
#
# Fast:
#   Fast feedback while Builder is iterating.
#
# Full:
#   Pre-review / pre-PR verification.
#
# This script DOES NOT:
#   - install dependencies
#   - modify source code
#   - commit changes
#   - push changes
# ============================================================


# ============================================================
# RESULT STATE
# ============================================================

$script:Passed = @()
$script:Skipped = @()
$script:Failed = @()


# ============================================================
# DISPLAY HELPERS
# ============================================================

function Write-Header {
    param(
        [string]$Message
    )

    Write-Host ""
    Write-Host "============================================================"
    Write-Host " $Message"
    Write-Host "============================================================"
    Write-Host ""
}


function Write-Step {
    param(
        [string]$Message
    )

    Write-Host ""
    Write-Host "------------------------------------------------------------"
    Write-Host $Message
    Write-Host "------------------------------------------------------------"
}


function Add-Pass {
    param(
        [string]$Name
    )

    $script:Passed += $Name
    Write-Host "  PASS  $Name"
}


function Add-Skip {
    param(
        [string]$Name,
        [string]$Reason
    )

    $script:Skipped += "$Name - $Reason"

    Write-Host "  SKIP  $Name"
    Write-Host "        $Reason"
}


function Add-Failure {
    param(
        [string]$Name,
        [string]$Reason
    )

    $script:Failed += "$Name - $Reason"

    Write-Host "  FAIL  $Name"
    Write-Host "        $Reason"
}


# ============================================================
# COMMAND HELPERS
# ============================================================

function Test-CommandExists {
    param(
        [string]$Name
    )

    $command = Get-Command -Name $Name -ErrorAction SilentlyContinue

    if ($null -eq $command) {
        return $false
    }

    return $true
}


function Invoke-Check {
    param(
        [string]$Name,
        [scriptblock]$Action
    )

    Write-Host ""
    Write-Host "  Running: $Name"

    try {
        $global:LASTEXITCODE = 0

        & $Action

        $exitCode = $global:LASTEXITCODE

        if ($null -eq $exitCode) {
            $exitCode = 0
        }

        if ($exitCode -eq 0) {
            Add-Pass $Name
        }
        else {
            Add-Failure $Name "Exited with code $exitCode"
        }
    }
    catch {
        Add-Failure $Name $_.Exception.Message
    }
}


# ============================================================
# NODE HELPERS
# ============================================================

function Get-PackageScripts {

    if (-not (Test-Path -LiteralPath "package.json")) {
        return @()
    }

    try {
        $package = Get-Content -LiteralPath "package.json" -Raw |
            ConvertFrom-Json

        if ($null -eq $package.scripts) {
            return @()
        }

        return @(
            $package.scripts.PSObject.Properties.Name
        )
    }
    catch {
        Add-Failure "Read package.json" $_.Exception.Message
        return @()
    }
}


function Invoke-NodeScript {
    param(
        [string]$PackageManager,
        [string]$ScriptName,
        [string[]]$AvailableScripts
    )

    if ($AvailableScripts -notcontains $ScriptName) {
        Add-Skip "Node: $ScriptName" "Script not defined in package.json."
        return
    }

    Invoke-Check "Node: $ScriptName" {

        if ($PackageManager -eq "pnpm") {
            & pnpm run $ScriptName
        }
        elseif ($PackageManager -eq "yarn") {
            & yarn run $ScriptName
        }
        else {
            & npm run $ScriptName
        }
    }
}


# ============================================================
# START
# ============================================================

Write-Header "AI ENGINEERING QUALITY GATE"

Write-Host "Mode: $Mode"
Write-Host ""


# ============================================================
# RESOLVE REPOSITORY ROOT
# ============================================================

$Root = $env:ZED_WORKTREE_ROOT

if ([string]::IsNullOrWhiteSpace($Root)) {

    if (Test-CommandExists "git") {

        $GitRoot = git rev-parse --show-toplevel 2>$null

        if (
            ($LASTEXITCODE -eq 0) -and
            (-not [string]::IsNullOrWhiteSpace($GitRoot))
        ) {
            $Root = $GitRoot.Trim()
        }
    }

    if ([string]::IsNullOrWhiteSpace($Root)) {
        $Root = (Get-Location).Path
    }
}


$Root = [System.IO.Path]::GetFullPath($Root)

Set-Location -LiteralPath $Root


Write-Host "Repository:"
Write-Host "  $Root"
Write-Host ""


# ============================================================
# 1. GIT / REPOSITORY SANITY
# ============================================================

Write-Step "[1/8] Repository sanity"


if (-not (Test-CommandExists "git")) {

    Add-Failure "Git availability" "git is not available in PATH."
}
else {

    $insideGit = git rev-parse --is-inside-work-tree 2>$null

    if (
        ($LASTEXITCODE -ne 0) -or
        ($insideGit -ne "true")
    ) {

        Add-Failure "Git repository" "Current directory is not a Git repository."
    }
    else {

        Add-Pass "Git repository"

        Invoke-Check "Git: diff --check" {
            git diff --check
        }

        $branch = git branch --show-current
        $commit = git rev-parse --short HEAD

        Write-Host ""

        if ([string]::IsNullOrWhiteSpace($branch)) {
            Write-Host "  Branch: DETACHED HEAD"
        }
        else {
            Write-Host "  Branch: $branch"
        }

        Write-Host "  Commit: $commit"
    }
}


# ============================================================
# 2. AI ENGINEERING SCAFFOLD
# ============================================================

Write-Step "[2/8] AI engineering scaffold"


$requiredFiles = @(
    "AGENTS.md",
    "ARCHITECTURE.md"
)


foreach ($file in $requiredFiles) {

    if (Test-Path -LiteralPath $file) {
        Add-Pass "Scaffold: $file"
    }
    else {
        Add-Failure "Scaffold: $file" "Required workflow file is missing."
    }
}


$requiredDirectories = @(
    ".agents\skills"
)


foreach ($directory in $requiredDirectories) {

    if (Test-Path -LiteralPath $directory) {
        Add-Pass "Scaffold: $directory"
    }
    else {
        Add-Failure "Scaffold: $directory" "Required workflow directory is missing."
    }
}


$optionalDirectories = @(
    "docs\plans\active",
    "docs\plans\completed",
    "docs\decisions"
)


foreach ($directory in $optionalDirectories) {

    if (Test-Path -LiteralPath $directory) {
        Add-Pass "Scaffold: $directory"
    }
    else {
        Add-Skip "Scaffold: $directory" "Optional workflow directory not present."
    }
}


# ============================================================
# 3. JAVASCRIPT / TYPESCRIPT
# ============================================================

Write-Step "[3/8] JavaScript / TypeScript"


$HasNodeProject = Test-Path -LiteralPath "package.json"


if ($HasNodeProject) {

    $PackageManager = $null


    if (Test-Path -LiteralPath "pnpm-lock.yaml") {
        $PackageManager = "pnpm"
    }
    elseif (Test-Path -LiteralPath "yarn.lock") {
        $PackageManager = "yarn"
    }
    elseif (Test-Path -LiteralPath "package-lock.json") {
        $PackageManager = "npm"
    }
    else {
        $PackageManager = "npm"
        Write-Warning "Node project detected without a recognized lockfile."
    }


    if (-not (Test-CommandExists $PackageManager)) {

        Add-Failure `
            "Node package manager" `
            "$PackageManager is not installed or available in PATH."
    }
    else {

        Write-Host "  Package manager: $PackageManager"

        $Scripts = Get-PackageScripts


        # ----------------------------------------------------
        # Fast checks
        # ----------------------------------------------------

        Invoke-NodeScript `
            $PackageManager `
            "typecheck" `
            $Scripts


        Invoke-NodeScript `
            $PackageManager `
            "lint" `
            $Scripts


        if ($Scripts -contains "test:unit") {

            Invoke-NodeScript `
                $PackageManager `
                "test:unit" `
                $Scripts
        }
        elseif ($Scripts -contains "test") {

            Invoke-NodeScript `
                $PackageManager `
                "test" `
                $Scripts
        }
        else {

            Add-Skip `
                "Node: tests" `
                "No test or test:unit script is defined."
        }


        # ----------------------------------------------------
        # Full checks
        # ----------------------------------------------------

        if ($Mode -eq "Full") {

            if ($Scripts -contains "test:integration") {

                Invoke-NodeScript `
                    $PackageManager `
                    "test:integration" `
                    $Scripts
            }


            if ($Scripts -contains "test:e2e") {

                Invoke-NodeScript `
                    $PackageManager `
                    "test:e2e" `
                    $Scripts
            }


            if ($Scripts -contains "check") {

                Invoke-NodeScript `
                    $PackageManager `
                    "check" `
                    $Scripts
            }


            Invoke-NodeScript `
                $PackageManager `
                "build" `
                $Scripts
        }
    }
}
else {

    Add-Skip `
        "JavaScript / TypeScript" `
        "No package.json detected."
}


# ============================================================
# 4. PYTHON
# ============================================================

Write-Step "[4/8] Python"


$HasPyProject = Test-Path -LiteralPath "pyproject.toml"
$HasRequirements = Test-Path -LiteralPath "requirements.txt"
$HasUvLock = Test-Path -LiteralPath "uv.lock"

$HasPythonProject = (
    $HasPyProject -or
    $HasRequirements -or
    $HasUvLock
)


if ($HasPythonProject) {

    $HasUv = Test-CommandExists "uv"
    $HasRuff = Test-CommandExists "ruff"
    $HasPytest = Test-CommandExists "pytest"
    $HasPython = Test-CommandExists "python"
    $HasPyright = Test-CommandExists "pyright"
    $HasMypy = Test-CommandExists "mypy"


    # --------------------------------------------------------
    # Ruff
    # --------------------------------------------------------

    if ($HasRuff) {

        Invoke-Check "Python: ruff" {
            ruff check .
        }
    }
    elseif ($HasUv -and $HasPyProject) {

        $PyProjectText = Get-Content `
            -LiteralPath "pyproject.toml" `
            -Raw `
            -ErrorAction SilentlyContinue

        if (
            ($null -ne $PyProjectText) -and
            ($PyProjectText -match "ruff")
        ) {

            Invoke-Check "Python: ruff" {
                uv run ruff check .
            }
        }
        else {

            Add-Skip `
                "Python: ruff" `
                "ruff does not appear to be configured."
        }
    }
    else {

        Add-Skip `
            "Python: ruff" `
            "ruff is not available."
    }


    # --------------------------------------------------------
    # Python Tests
    # --------------------------------------------------------

    $HasTestsDirectory = Test-Path -LiteralPath "tests"

    $HasTestFiles = $false

    $TestFile = Get-ChildItem `
        -Path "." `
        -Filter "test_*.py" `
        -Recurse `
        -File `
        -ErrorAction SilentlyContinue |
        Select-Object -First 1

    if ($null -ne $TestFile) {
        $HasTestFiles = $true
    }


    if ($HasTestsDirectory -or $HasTestFiles) {

        if ($HasPytest) {

            Invoke-Check "Python: pytest" {
                pytest
            }
        }
        elseif ($HasUv -and $HasPyProject) {

            Invoke-Check "Python: pytest" {
                uv run pytest
            }
        }
        elseif ($HasPython) {

            Invoke-Check "Python: pytest" {
                python -m pytest
            }
        }
        else {

            Add-Failure `
                "Python: pytest" `
                "Tests exist, but no pytest execution method is available."
        }
    }
    else {

        Add-Skip `
            "Python: tests" `
            "No tests directory or test_*.py files detected."
    }


    # --------------------------------------------------------
    # Full Python Type Checking
    # --------------------------------------------------------

    if ($Mode -eq "Full") {

        if ($HasPyright) {

            Invoke-Check "Python: pyright" {
                pyright
            }
        }
        elseif ($HasMypy) {

            Invoke-Check "Python: mypy" {
                mypy .
            }
        }
        elseif ($HasUv -and $HasPyProject) {

            $PyProjectText = Get-Content `
                -LiteralPath "pyproject.toml" `
                -Raw `
                -ErrorAction SilentlyContinue


            if (
                ($null -ne $PyProjectText) -and
                ($PyProjectText -match "pyright")
            ) {

                Invoke-Check "Python: pyright" {
                    uv run pyright
                }
            }
            elseif (
                ($null -ne $PyProjectText) -and
                ($PyProjectText -match "mypy")
            ) {

                Invoke-Check "Python: mypy" {
                    uv run mypy .
                }
            }
            else {

                Add-Skip `
                    "Python: typecheck" `
                    "No Python type checker appears to be configured."
            }
        }
        else {

            Add-Skip `
                "Python: typecheck" `
                "No Python type checker detected."
        }
    }
}
else {

    Add-Skip `
        "Python" `
        "No Python project manifest detected."
}


# ============================================================
# 5. GO
# ============================================================

Write-Step "[5/8] Go"


# Mirrors scripts/quality.sh (lint, unit, race, integration, build, vuln, tracing), with these DIFFERENCES, all
# deliberate and none hidden:
#   - Fast mode runs gofmt, go test, go vet only; everything else below is Full mode only (quality.sh `lint` runs
#     golangci-lint on its own, here it is a Full-mode stage).
#   - `observability` (scripts/promtool.sh is bash) and `deploy` (hadolint, shellcheck, `docker compose config`, bash
#     scripts) are NOT mirrored and are reported as skips in Full mode; run `bash scripts/quality.sh observability` and
#     `bash scripts/quality.sh deploy` from Git Bash or WSL.
#   - `go test -race` needs cgo, so it runs only when gcc is on PATH (else a loud skip; the must-run stages then run
#     without -race). quality.sh always uses -race.
#   - govulncheck is part of Full here; in quality.sh it is its own stage that `full` does not run. A missing
#     govulncheck or golangci-lint is a failure here (this script installs nothing); quality.sh installs govulncheck.
#   - On Windows the ./tests/integration package (process tests that send SIGSTOP/SIGTERM) does not compile
#     (syscall.SIGSTOP is undefined), so it is left out of the must-run stages and the named tests that live in it are
#     not required; the skip is announced. Fixing that needs a platform split of those tests, not a one-line constraint.
#   - Integration runs only when all of SERVERFLOW_TEST_POSTGRES_DSN, _REDIS_ADDR and _KAFKA_BROKERS are set (a loud
#     skip otherwise, like `quality.sh full`).
# Nothing here has been run on Windows by the people who wrote it; internal/ciguard only checks that the stage
# invocations and test names have not drifted from quality.sh (it cannot check the PowerShell is correct).
# Line endings: the repository has no .gitattributes; a checkout with CRLF makes `gofmt -l` list every .go file on
# Windows (git's core.autocrlf=input or a .gitattributes with `*.go text eol=lf` avoids it). Unverified here.

# The named tests that live in ./tests/integration, which does not compile on Windows (syscall.SIGSTOP is undefined).
$WindowsExcludedTests = @(
    "TestADeadCollectorNeverTouchesRequests",
    "TestARetriedRequestShowsTwoWorkerAttemptsInOneTrace",
    "TestGatewayEventsEndToEnd",
    "TestGatewayWithRealPostgresHotPathAndFloods",
    "TestOneTraceAcrossGatewayAndWorker",
    "TestProcessAuthEndToEnd",
    "TestProcessGatewaySIGTERMFlushesTheTerminalEventsOfInflightRequests",
    "TestProcessRateLimitingEndToEnd",
    "TestProcessUsageConsumerEndToEnd",
    "TestRedisFailureMatrixThroughTheGateway",
    "TestThreeGatewaysShareOneRequestQuota",
    "TestUsageCrashBetweenDatabaseAndOffsetCommit",
    "TestUsageStopAndReplay"
)


function Assert-NativeSucceeded {
    param(
        [string]$What
    )

    if ($LASTEXITCODE -ne 0) {
        throw "$What exited with code $LASTEXITCODE"
    }
}


# Runs `go test` verbosely with its output in a log file, then fails if the test run failed, if any test skipped for
# want of its server ($SkipMessage), or if a named test did not run and pass. A silent skip must not look like green.
function Invoke-GoTestMustRun {
    param(
        [string]$Label,
        [string[]]$GoArgs,
        [string]$SkipMessage,
        [string[]]$Tests
    )

    $log = Join-Path ([System.IO.Path]::GetTempPath()) ("serverflow-$Label-" + [guid]::NewGuid().ToString("N") + ".log")

    # ./tests/integration does not compile on Windows (see the header of this section): drop it, and the tests that live
    # in it, and say so.
    if ($env:OS -eq "Windows_NT") {
        $GoArgs = @($GoArgs | Where-Object { $_ -ne "./tests/integration/..." })

        $skipped = @($Tests | Where-Object { $WindowsExcludedTests -contains $_ })

        if ($skipped.Count -gt 0) {
            Write-Host "  NOTE  $Label`: ./tests/integration does not compile on Windows; not run: $($skipped -join ', ')"
        }

        $Tests = @($Tests | Where-Object { $WindowsExcludedTests -notcontains $_ })
    }

    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"

    try {
        & go test @GoArgs 2>&1 | ForEach-Object { "$_" } | Out-File -FilePath $log -Encoding utf8
        $code = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousPreference
    }

    if ($code -ne 0) {
        Write-Host "--- failed tests, packages, panics and races in the full log ---"
        Select-String -Path $log -Pattern '^(--- FAIL|    --- FAIL|FAIL|panic:|WARNING: DATA RACE)' -Context 0, 12 |
            Select-Object -First 100 |
            ForEach-Object {
                Write-Host $_.Line
                $_.Context.PostContext | ForEach-Object { Write-Host $_ }
            }
        Write-Host "--- last 80 lines ---"
        Get-Content -LiteralPath $log -Tail 80 | ForEach-Object { Write-Host $_ }
        throw "$Label tests failed (full log kept at $log)"
    }

    if (($SkipMessage -ne "") -and (Select-String -Path $log -Pattern $SkipMessage -SimpleMatch -Quiet)) {
        throw "$Label tests were skipped ($SkipMessage); the server is not configured (log: $log)"
    }

    foreach ($test in $Tests) {
        if (-not (Select-String -Path $log -Pattern "--- PASS: $test " -SimpleMatch -Quiet)) {
            throw "expected $test to run and pass (log: $log)"
        }
    }

    Remove-Item -LiteralPath $log -Force -ErrorAction SilentlyContinue
    Write-Host "  $Label`: $($Tests.Count) named tests ran and passed"
}


if (Test-Path -LiteralPath "go.mod") {

    if (-not (Test-CommandExists "go")) {

        Add-Failure "Go" "Go is not installed."
    }
    else {

        # The packages `go test` and `go vet` cover. On Windows ./tests/integration cannot even be compiled (see the
        # header of this section), so it is left out there and the omission is announced.
        $GoPackages = @("./...")

        if ($env:OS -eq "Windows_NT") {
            $GoPackages = @(go list ./... | Where-Object { $_ -notlike "*/tests/integration" })
            Write-Host "  NOTE  Windows: ./tests/integration does not compile here (syscall.SIGSTOP is undefined) and is not part of go test, go vet or go test -race."
        }

        # ----------------------------------------------------
        # Fast checks (also the start of Full)
        # ----------------------------------------------------

        Invoke-Check "Go: gofmt" {
            $unformatted = gofmt -l .

            if (-not [string]::IsNullOrWhiteSpace(($unformatted -join ""))) {
                $unformatted | ForEach-Object { Write-Host "    $_" }
                throw "gofmt: the files above are not formatted (run gofmt -w .)"
            }
        }


        Invoke-Check "Go: test" {
            go test @GoPackages
        }


        Invoke-Check "Go: vet" {
            go vet @GoPackages
        }


        # ----------------------------------------------------
        # Full checks
        # ----------------------------------------------------

        if ($Mode -eq "Full") {

            if (Test-CommandExists "golangci-lint") {
                Invoke-Check "Go: golangci-lint" {
                    golangci-lint run ./...
                }
            }
            else {
                Add-Failure `
                    "Go: golangci-lint" `
                    "golangci-lint is not installed (https://golangci-lint.run/welcome/install/)."
            }


            # The race detector needs cgo, which on Windows means a C compiler (gcc) on PATH.
            $RaceFlags = @()

            if (Test-CommandExists "gcc") {
                $RaceFlags = @("-race")

                Invoke-Check "Go: go test -race" {
                    go test -race @GoPackages
                }
            }
            else {
                Add-Skip `
                    "Go: go test -race" `
                    "The race detector needs cgo and no C compiler (gcc) is on PATH; install one (for example MinGW-w64) or run it in CI. The integration and tracing tests below run without -race."
            }


            Invoke-Check "Go: tracing tests must run, not skip" {
                Invoke-GoTestMustRun `
                    -Label "tracing" `
                    -GoArgs (@($RaceFlags) + @("-count=1", "-v", "./internal/tracing/...", "./internal/gateway/...", "./internal/mockworker/...", "./internal/config/...", "./tests/integration/...")) `
                    -SkipMessage "" `
                    -Tests @(
                        "TestOneTraceAcrossGatewayAndWorker",
                        "TestARetriedRequestShowsTwoWorkerAttemptsInOneTrace",
                        "TestNoSecretsInSpansAndOnlyAllowListedKeys",
                        "TestADeadCollectorNeverTouchesRequests",
                        "TestGatewayDoesNotDependOnOpenTelemetry",
                        "TestExportReachesTheReceiver",
                        "TestStreamingRequestSpanTree",
                        "TestGatewayOverhead"
                    )
            }


            # Integration needs the test servers of docs/development/ci.md. Like `quality.sh full`, an unset variable is a
            # loud skip here (CI runs it), not a silent pass.
            $HasPostgres = -not [string]::IsNullOrWhiteSpace($env:SERVERFLOW_TEST_POSTGRES_DSN)
            $HasRedis = -not [string]::IsNullOrWhiteSpace($env:SERVERFLOW_TEST_REDIS_ADDR)
            $HasKafka = -not [string]::IsNullOrWhiteSpace($env:SERVERFLOW_TEST_KAFKA_BROKERS)

            if ($HasPostgres -and $HasRedis -and $HasKafka) {

                Invoke-Check "Go: integration (postgres) must run, not skip" {
                    Invoke-GoTestMustRun `
                        -Label "postgres" `
                        -GoArgs (@($RaceFlags) + @("-count=1", "-v", "./internal/postgres/...", "./internal/auth/...", "./cmd/admin/...", "./tests/integration/...")) `
                        -SkipMessage "SERVERFLOW_TEST_POSTGRES_DSN is not set" `
                        -Tests @(
                            "TestMigrateUpCreatesSchemaAndIsIdempotent",
                            "TestFullWorkflowAndKeyShownOnce",
                            "TestProcessAuthEndToEnd",
                            "TestGatewayWithRealPostgresHotPathAndFloods"
                        )
                }

                Invoke-Check "Go: integration (redis) must run, not skip" {
                    Invoke-GoTestMustRun `
                        -Label "redis" `
                        -GoArgs (@($RaceFlags) + @("-count=1", "-v", "./internal/redis/...", "./internal/ratelimit/...", "./internal/gateway/...", "./tests/integration/...")) `
                        -SkipMessage "SERVERFLOW_TEST_REDIS_ADDR is not set" `
                        -Tests @(
                            "TestRunAndPingWithPassword",
                            "TestRequestsBoundaryAgainstRedis",
                            "TestScriptMatchesModelOnRandomSequences",
                            "TestFailureClosedAcrossTheMatrix",
                            "TestThreeGatewaysShareOneRequestQuota",
                            "TestRedisFailureMatrixThroughTheGateway",
                            "TestProcessRateLimitingEndToEnd"
                        )
                }

                # Kafka (Phase 12). The usage tests also need the database.
                Invoke-Check "Go: integration (kafka) must run, not skip" {
                    Invoke-GoTestMustRun `
                        -Label "kafka" `
                        -GoArgs (@($RaceFlags) + @("-count=1", "-v", "./internal/kafka/...", "./internal/events/...", "./internal/usage/...", "./tests/integration/...")) `
                        -SkipMessage "SERVERFLOW_TEST_KAFKA_BROKERS is not set" `
                        -Tests @(
                            "TestProducerDeliversToRealBroker",
                            "TestBrokerOutageDoesNotBlockAndDropsAreCounted",
                            "TestConsumerGroupCommitsAfterDatabase",
                            "TestUsageStopAndReplay",
                            "TestGatewayEventsEndToEnd",
                            "TestUsageCrashBetweenDatabaseAndOffsetCommit",
                            "TestProcessGatewaySIGTERMFlushesTheTerminalEventsOfInflightRequests",
                            "TestProcessUsageConsumerEndToEnd"
                        )
                }

                Invoke-Check "Go: integration (observability families) must run, not skip" {
                    Invoke-GoTestMustRun `
                        -Label "observability" `
                        -GoArgs (@($RaceFlags) + @("-count=1", "-v", "./internal/observability/...")) `
                        -SkipMessage "is not set; skipping the Redis and PostgreSQL families check" `
                        -Tests @("TestRedisAndPostgresFamiliesAreExported")
                }
            }
            else {
                Add-Skip `
                    "Go: integration (postgres, redis, kafka)" `
                    "SERVERFLOW_TEST_POSTGRES_DSN, SERVERFLOW_TEST_REDIS_ADDR or SERVERFLOW_TEST_KAFKA_BROKERS is not set (see docs/development/ci.md). CI will run it."
            }


            Add-Skip `
                "Go: observability (promtool)" `
                "Not mirrored: scripts/promtool.sh is bash. Run: bash scripts/quality.sh observability"

            Add-Skip `
                "Go: deploy (hadolint, shellcheck, docker compose config)" `
                "Not mirrored: these are bash and container checks. Run: bash scripts/quality.sh deploy"


            Invoke-Check "Go: build" {
                go build ./...
            }


            foreach ($target in @("linux/amd64", "linux/arm64", "darwin/arm64", "windows/amd64")) {

                $targetOS = $target.Split("/")[0]
                $targetArch = $target.Split("/")[1]

                Invoke-Check "Go: cross-compile $target" {
                    $previousOS = $env:GOOS
                    $previousArch = $env:GOARCH

                    try {
                        $env:GOOS = $targetOS
                        $env:GOARCH = $targetArch
                        go build ./...
                    }
                    finally {
                        $env:GOOS = $previousOS
                        $env:GOARCH = $previousArch
                    }
                }
            }


            Invoke-Check "Go: go mod verify" {
                go mod verify
            }

            Invoke-Check "Go: go mod tidy (must change nothing)" {
                go mod tidy -diff
            }


            # govulncheck: any called vulnerability fails (no exception list, ADR-018). This script installs nothing.
            if (Test-CommandExists "govulncheck") {
                Invoke-Check "Go: govulncheck" {
                    govulncheck ./...
                }
            }
            else {
                Add-Failure `
                    "Go: govulncheck" `
                    "govulncheck is not installed (go install golang.org/x/vuln/cmd/govulncheck@v1.8.0)."
            }
        }
    }
}
else {

    Add-Skip `
        "Go" `
        "No go.mod detected."
}


# ============================================================
# 6. JAVA
# ============================================================

Write-Step "[6/8] Java"


if (Test-Path -LiteralPath "gradlew.bat") {

    if ($Mode -eq "Fast") {

        Invoke-Check "Gradle: test" {
            & ".\gradlew.bat" test
        }
    }
    else {

        Invoke-Check "Gradle: check" {
            & ".\gradlew.bat" check
        }


        Invoke-Check "Gradle: build" {
            & ".\gradlew.bat" build
        }
    }
}
elseif (Test-Path -LiteralPath "pom.xml") {

    if (-not (Test-CommandExists "mvn")) {

        Add-Failure "Maven" "mvn is not installed."
    }
    elseif ($Mode -eq "Fast") {

        Invoke-Check "Maven: test" {
            mvn test
        }
    }
    else {

        Invoke-Check "Maven: verify" {
            mvn verify
        }
    }
}
else {

    Add-Skip `
        "Java" `
        "No Gradle wrapper or pom.xml detected."
}


# ============================================================
# 7. RUST
# ============================================================

Write-Step "[7/8] Rust"


if (Test-Path -LiteralPath "Cargo.toml") {

    if (-not (Test-CommandExists "cargo")) {

        Add-Failure "Rust" "cargo is not installed."
    }
    else {

        Invoke-Check "Rust: cargo check" {
            cargo check
        }


        Invoke-Check "Rust: cargo test" {
            cargo test
        }


        if ($Mode -eq "Full") {

            Invoke-Check "Rust: cargo clippy" {
                cargo clippy -- -D warnings
            }


            Invoke-Check "Rust: cargo build" {
                cargo build
            }
        }
    }
}
else {

    Add-Skip `
        "Rust" `
        "No Cargo.toml detected."
}


# ============================================================
# 8. FINAL REPOSITORY INSPECTION
# ============================================================

Write-Step "[8/8] Final repository inspection"


if (Test-CommandExists "git") {

    Write-Host "  Changed files:"
    Write-Host ""

    $Changed = git status --short


    if ([string]::IsNullOrWhiteSpace(($Changed -join ""))) {

        Write-Host "    none"
    }
    else {

        $Changed | ForEach-Object {
            Write-Host "    $_"
        }
    }


    Write-Host ""


    Invoke-Check "Git: final diff --check" {
        git diff --check
    }
}


# ============================================================
# FINAL RESULT
# ============================================================

Write-Header "QUALITY GATE RESULT"


Write-Host "Mode: $Mode"
Write-Host ""


Write-Host "Passed:"

if ($Passed.Count -eq 0) {

    Write-Host "  none"
}
else {

    foreach ($item in $Passed) {
        Write-Host "  + $item"
    }
}


Write-Host ""
Write-Host "Skipped:"


if ($Skipped.Count -eq 0) {

    Write-Host "  none"
}
else {

    foreach ($item in $Skipped) {
        Write-Host "  - $item"
    }
}


Write-Host ""
Write-Host "Failed:"


if ($Failed.Count -eq 0) {

    Write-Host "  none"
}
else {

    foreach ($item in $Failed) {
        Write-Host "  X $item"
    }
}


Write-Host ""


if ($Failed.Count -gt 0) {

    Write-Host "============================================================"
    Write-Host "                  QUALITY GATE FAILED"
    Write-Host "============================================================"
    Write-Host ""

    exit 1
}


Write-Host "============================================================"
Write-Host "                  QUALITY GATE PASSED"
Write-Host "============================================================"
Write-Host ""

exit 0
