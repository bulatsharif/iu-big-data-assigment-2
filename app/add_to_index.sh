#!/bin/bash
set -euo pipefail

source .venv/bin/activate

if [ $# -ne 1 ]; then
    echo "Usage: bash add_to_index.sh <local_txt_file>"
    exit 1
fi

python3 add_to_index.py "$1"
