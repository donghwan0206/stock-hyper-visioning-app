#!/usr/bin/env pwsh
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Path $MyInvocation.MyCommand.Path -Parent
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
$PackageDir = Join-Path $RepoRoot "packages/kis_api"
$OutDir = Join-Path $RepoRoot "dist"

try {
    python -m build --wheel --outdir $OutDir $PackageDir
    Write-Host "Built kis_api wheel under $OutDir via python -m build"
}
catch {
    Write-Warning "python -m build 실패, pip wheel로 대체합니다. ($_ )"
    python -m pip wheel $PackageDir -w $OutDir
    Write-Host "Built kis_api wheel under $OutDir via pip wheel"
}
