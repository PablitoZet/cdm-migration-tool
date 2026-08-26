$ErrorActionPreference = "Stop"
py -3 -m venv .venv
& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install --requirement requirements-dev.txt
& .\.venv\Scripts\python.exe -m unittest discover -s tests -v
if (-not (Test-Path .\config.json)) {
    Copy-Item .\config.example.json .\config.json
}
Write-Host "Bootstrap complete. Run '.\.venv\Scripts\python.exe app.py' and open http://127.0.0.1:8110."
