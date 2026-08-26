#!/usr/bin/env sh
set -eu
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install --requirement requirements-dev.txt
python -m unittest discover -s tests -v
if [ ! -f config.json ]; then
  cp config.example.json config.json
fi
chmod 600 config.json
echo "Bootstrap complete. Run '.venv/bin/python app.py' and open http://127.0.0.1:8110."
