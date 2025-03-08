#!/bin/bash
set -e

if [ ! -d ".venv_dev" ]; then
    echo "Virtual environment not found. Running build script..."
    ./scripts/build_dev.sh
fi

source .venv_dev/bin/activate

# Type Check
python -m mypy

# Doctype Check
darglint src/valkey_dict/

# Multiple linters
python -m pylama -i E501,E231 src

# Unit tests
python -m unittest discover -s tests

# Security Check
 bandit -r src/valkey_dict

# Docstring Check
pydocstyle src/valkey_dict/

deactivate
