# ============================================================
# Zed AI Worktree Initialization
# ============================================================

$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)

    Write-Host ""
    Write-Host "------------------------------------------------------------"
    Write-Host $Message
    Write-Host "------------------------------------------------------------"
}

function Command-Exists {
    param([string]$Command)

    return [bool](Get-Command $Command -ErrorAction SilentlyContinue)
}

function Run-Command {
    param(
        [string]$Description,
        [scriptblock]$Command
    )

    Write-Host "  -> $Description"

    & $Command

    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}


# ============================================================
# START
# ============================================================

Write-Host ""
Write-Host "============================================================"
Write-Host "            AI WORKTREE INITIALIZATION"
Write-Host "============================================================"
Write-Host ""


# ============================================================
# 1. RESOLVE WORKTREE
# ============================================================

$Root = $env:ZED_WORKTREE_ROOT
$MainRoot = $env:ZED_MAIN_GIT_WORKTREE

if ([string]::IsNullOrWhiteSpace($Root)) {
    $Root = (Get-Location).Path

    Write-Host "ZED_WORKTREE_ROOT not detected."
    Write-Host "Running in MANUAL TEST MODE."
    Write-Host "Using current directory as worktree root."
    Write-Host ""
}

$Root = [System.IO.Path]::GetFullPath($Root)

Write-Host "Worktree:"
Write-Host "  $Root"

if (-not [string]::IsNullOrWhiteSpace($MainRoot)) {
    Write-Host ""
    Write-Host "Main Git worktree:"
    Write-Host "  $MainRoot"
}

if (-not (Test-Path -LiteralPath $Root)) {
    throw "Worktree path does not exist: $Root"
}

Set-Location -LiteralPath $Root


# ============================================================
# 2. VERIFY GIT
# ============================================================

Write-Step "[1/6] Verifying Git worktree"

if (-not (Command-Exists "git")) {
    throw "Git is not installed or is not available in PATH."
}

$insideGit = git rev-parse --is-inside-work-tree 2>$null

if ($LASTEXITCODE -ne 0 -or $insideGit -ne "true") {
    throw "Current directory is not a valid Git worktree."
}

$commit = git rev-parse --short HEAD
$branch = git branch --show-current

Write-Host "  Git repository: OK"
Write-Host "  HEAD: $commit"

if ([string]::IsNullOrWhiteSpace($branch)) {
    Write-Host "  Branch: DETACHED HEAD"
    Write-Host ""
    Write-Host "  Create an agent branch before implementation:"
    Write-Host "    git switch -c agent/<task-name>"
}
else {
    Write-Host "  Branch: $branch"
}


# ============================================================
# 3. VERIFY AI SCAFFOLD
# ============================================================

Write-Step "[2/6] Verifying AI engineering scaffold"

$requiredFiles = @(
    "AGENTS.md",
    "ARCHITECTURE.md"
)

foreach ($file in $requiredFiles) {
    if (Test-Path -LiteralPath $file) {
        Write-Host "  OK      $file"
    }
    else {
        Write-Warning "MISSING $file"
    }
}

$requiredDirectories = @(
    ".agents\skills",
    "docs\plans\active",
    "docs\plans\completed",
    "docs\decisions",
    ".zed"
)

foreach ($directory in $requiredDirectories) {
    if (Test-Path -LiteralPath $directory) {
        Write-Host "  OK      $directory"
    }
    else {
        Write-Warning "MISSING $directory"
    }
}


# ============================================================
# 4. DETECT PROJECT + INSTALL DEPENDENCIES
# ============================================================

Write-Step "[3/6] Detecting project environment"

$DetectedProject = $false


# Node - pnpm

if (Test-Path -LiteralPath "pnpm-lock.yaml") {
    $DetectedProject = $true

    Write-Host "  Detected: Node.js / pnpm"

    if (Command-Exists "pnpm") {
        Run-Command "Installing pnpm dependencies" {
            pnpm install --frozen-lockfile
        }
    }
    else {
        Write-Warning "pnpm is not installed."
    }
}


# Node - npm

elseif (Test-Path -LiteralPath "package-lock.json") {
    $DetectedProject = $true

    Write-Host "  Detected: Node.js / npm"

    if (Command-Exists "npm") {
        Run-Command "Installing npm dependencies" {
            npm ci
        }
    }
    else {
        Write-Warning "npm is not installed."
    }
}


# Node - yarn

elseif (Test-Path -LiteralPath "yarn.lock") {
    $DetectedProject = $true

    Write-Host "  Detected: Node.js / Yarn"

    if (Command-Exists "yarn") {
        Run-Command "Installing Yarn dependencies" {
            yarn install --immutable
        }
    }
    else {
        Write-Warning "Yarn is not installed."
    }
}


# package.json without lockfile

elseif (Test-Path -LiteralPath "package.json") {
    $DetectedProject = $true

    Write-Host "  Detected: Node.js"
    Write-Warning "No lockfile detected. Dependency installation skipped."
}


# Python - uv

if (Test-Path -LiteralPath "uv.lock") {
    $DetectedProject = $true

    Write-Host "  Detected: Python / uv"

    if (Command-Exists "uv") {
        Run-Command "Synchronizing Python environment" {
            uv sync
        }
    }
    else {
        Write-Warning "uv is not installed."
    }
}


# Python - pyproject

elseif (Test-Path -LiteralPath "pyproject.toml") {
    $DetectedProject = $true

    Write-Host "  Detected: Python / pyproject.toml"
    Write-Host "  No uv.lock detected. Automatic install skipped."
}


# Python - requirements

elseif (Test-Path -LiteralPath "requirements.txt") {
    $DetectedProject = $true

    Write-Host "  Detected: Python / requirements.txt"
    Write-Host "  Automatic pip installation skipped."
}


# Java - Gradle

if (Test-Path -LiteralPath "gradlew.bat") {
    $DetectedProject = $true

    Write-Host "  Detected: Java / Gradle"

    Run-Command "Preparing Gradle project" {
        & ".\gradlew.bat" dependencies
    }
}


# Java - Maven

if (Test-Path -LiteralPath "pom.xml") {
    $DetectedProject = $true

    Write-Host "  Detected: Java / Maven"

    if (Command-Exists "mvn") {
        Run-Command "Preparing Maven dependencies" {
            mvn dependency:go-offline
        }
    }
    else {
        Write-Warning "Maven is not installed."
    }
}


# Go

if (Test-Path -LiteralPath "go.mod") {
    $DetectedProject = $true

    Write-Host "  Detected: Go"

    if (Command-Exists "go") {
        Run-Command "Downloading Go modules" {
            go mod download
        }
    }
    else {
        Write-Warning "Go is not installed."
    }
}


# Rust

if (Test-Path -LiteralPath "Cargo.toml") {
    $DetectedProject = $true

    Write-Host "  Detected: Rust"

    if (Command-Exists "cargo") {
        Run-Command "Fetching Cargo dependencies" {
            cargo fetch
        }
    }
    else {
        Write-Warning "Cargo is not installed."
    }
}


if (-not $DetectedProject) {
    Write-Host ""
    Write-Host "  No application manifest detected."
    Write-Host "  This is expected for the current AI workflow scaffold."
}


# ============================================================
# 5. BASELINE VALIDATION
# ============================================================

Write-Step "[4/6] Running baseline validation"

$ValidationRan = $false


# Node validation

if (
    (Test-Path -LiteralPath "package.json") -and
    (Command-Exists "npm")
) {
    try {
        $package = Get-Content -LiteralPath "package.json" -Raw |
            ConvertFrom-Json

        if ($null -ne $package.scripts) {
            $scriptNames = @(
                $package.scripts.PSObject.Properties.Name
            )

            if ($scriptNames -contains "typecheck") {
                $ValidationRan = $true

                Run-Command "Running typecheck" {
                    npm run typecheck
                }
            }

            if ($scriptNames -contains "lint") {
                $ValidationRan = $true

                Run-Command "Running lint" {
                    npm run lint
                }
            }
        }
    }
    catch {
        Write-Warning "Unable to inspect package.json scripts."
    }
}


# Go tests

if (
    (Test-Path -LiteralPath "go.mod") -and
    (Command-Exists "go")
) {
    $ValidationRan = $true

    Run-Command "Running Go tests" {
        go test ./...
    }
}


# Rust check

if (
    (Test-Path -LiteralPath "Cargo.toml") -and
    (Command-Exists "cargo")
) {
    $ValidationRan = $true

    Run-Command "Running cargo check" {
        cargo check
    }
}


if (-not $ValidationRan) {
    Write-Host "  No automatic baseline validation configured yet."
}


# ============================================================
# 6. GIT CLEANLINESS
# ============================================================

Write-Step "[5/6] Checking worktree state"

$status = git status --short

if ([string]::IsNullOrWhiteSpace(($status -join ""))) {
    Write-Host "  Working tree is clean."
}
else {
    Write-Host "  Working tree contains changes:"
    Write-Host ""

    $status | ForEach-Object {
        Write-Host "    $_"
    }
}

Write-Host ""
Write-Host "  Running git diff --check..."

git diff --check

if ($LASTEXITCODE -eq 0) {
    Write-Host "  No whitespace errors detected."
}
else {
    Write-Warning "git diff --check detected problems."
}


# ============================================================
# 7. SUMMARY
# ============================================================

Write-Step "[6/6] Initialization summary"

$currentBranch = git branch --show-current
$currentCommit = git rev-parse --short HEAD

Write-Host "  Worktree: $Root"
Write-Host "  Commit:   $currentCommit"

if ([string]::IsNullOrWhiteSpace($currentBranch)) {
    Write-Host "  Branch:   DETACHED HEAD"
}
else {
    Write-Host "  Branch:   $currentBranch"
}

Write-Host ""
Write-Host "============================================================"
Write-Host "          WORKTREE READY FOR AGENT EXECUTION"
Write-Host "============================================================"
Write-Host ""

if ([string]::IsNullOrWhiteSpace($currentBranch)) {
    Write-Host "Recommended next command:"
    Write-Host ""
    Write-Host "  git switch -c agent/<task-name>"
    Write-Host ""
}

exit 0
