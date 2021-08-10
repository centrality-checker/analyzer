# /bin/bash

set -ex

git pull --ff-only
git --git-dir=../storage/.git pull --ff-only

npm ci
npm run collect

pip3 install -r requirements.txt
python3 ./src/centrality/main.py

cd ../storage
git add .
git pull --ff-only
git commit -m "Update the data until $(cat ./registry/npm/last_time_scope) (sequence: $(cat ./registry/npm/sequence))"
git push
