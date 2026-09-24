$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

if (Test-Path "build/classes") {
    Remove-Item -Recurse -Force "build/classes"
}
New-Item -ItemType Directory -Path "build/classes" | Out-Null

$sources = Get-ChildItem -Path "src/main/java" -Filter "*.java" -Recurse | ForEach-Object { $_.FullName }
javac -Xlint:all -d build/classes $sources

if ($LASTEXITCODE -eq 0) {
    Write-Host "Compiled successfully to $(Get-Location)\build\classes"
} else {
    Write-Error "Compilation failed."
    exit $LASTEXITCODE
}
