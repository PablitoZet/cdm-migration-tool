#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")"
if [ ! -x .venv/bin/python ]; then
  ./bootstrap.sh
fi
if [ ! -f config.json ]; then
  cp config.example.json config.json
  chmod 600 config.json
fi
exec .venv/bin/python app.py
