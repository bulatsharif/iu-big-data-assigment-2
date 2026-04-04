#!/bin/bash
set -euo pipefail

# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
rm -rf .venv .venv.tar.gz
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -f -o .venv.tar.gz

# Collect data
bash prepare_data.sh


# Run the indexer
bash index.sh

# Run the ranker
bash search.sh "history"
bash search.sh "film"
bash search.sh "music"

tail -f /dev/null
