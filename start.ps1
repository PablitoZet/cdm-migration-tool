$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
if (-not (Test-Path .\.venv\Scripts\python.exe)) {
    & .\bootstrap.ps1
}
if (-not (Test-Path .\config.json)) {
    Copy-Item .\config.example.json .\config.json
}
& .\.venv\Scripts\python.exe .\app.py
