$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

if (Test-Path "build/classes") {
    Remove-Item -Recurse -Force "build/classes"
}
New-Item -ItemType Directory -Path "build/classes" | Out-Null

$sources = Get-ChildItem -Path "src/main/java" -Filter "*.java" -Recurse | ForEach-Object { $_.FullName }
# javac prints -Xlint warnings on stderr. When this script's output is redirected, Windows
# PowerShell 5.1 turns each stderr line into an error record, which "Stop" would treat as a
# failure; success is decided by javac's exit code instead.
$ErrorActionPreference = "Continue"
javac -Xlint:all -d build/classes $sources
$javacExit = $LASTEXITCODE
$ErrorActionPreference = "Stop"

if ($javacExit -eq 0) {
    # Resources (the document corpus) are loaded from the classpath by every node.
    if (Test-Path "src/main/resources") {
        Copy-Item -Recurse -Force "src/main/resources/*" "build/classes"
    }
    Write-Host "Compiled successfully to $(Get-Location)\build\classes"
} else {
    Write-Error "Compilation failed."
    exit $javacExit
}
