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
        $LASTEXITCODE = 0

        & $Action

        $exitCode = $LASTEXITCODE

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


if (Test-Path -LiteralPath "go.mod") {

    if (-not (Test-CommandExists "go")) {

        Add-Failure "Go" "Go is not installed."
    }
    else {

        Invoke-Check "Go: test" {
            go test ./...
        }


        Invoke-Check "Go: vet" {
            go vet ./...
        }


        if ($Mode -eq "Full") {

            Invoke-Check "Go: build" {
                go build ./...
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
