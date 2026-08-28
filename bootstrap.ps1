$ErrorActionPreference = "Stop"

function Assert-NativeSuccess {
    param(
        [string]$Step,
        [int]$ExitCode
    )
    if ($ExitCode -ne 0) {
        throw "$Step failed with exit code $ExitCode."
    }
}

py -3 -m venv .venv
Assert-NativeSuccess "Virtual environment creation" $LASTEXITCODE
& .\.venv\Scripts\python.exe -m pip install --upgrade pip
Assert-NativeSuccess "pip upgrade" $LASTEXITCODE
& .\.venv\Scripts\python.exe -m pip install --requirement requirements-dev.txt
Assert-NativeSuccess "Dependency installation" $LASTEXITCODE
& .\.venv\Scripts\python.exe -m unittest discover -s tests -v
Assert-NativeSuccess "Unit tests" $LASTEXITCODE
if (-not (Test-Path .\config.json)) {
    Copy-Item .\config.example.json .\config.json
}
Write-Host "Bootstrap complete. Run '.\.venv\Scripts\python.exe app.py' and open http://127.0.0.1:8110."
