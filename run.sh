#!/usr/bin/env bash
# Run from any directory. Bash in GitHub Codespaces or Linux/WSL with Docker.
set -eu
cd "$(dirname "$0")"
mkdir -p output
image=bdcc-validator:p2
image_id=$(docker image inspect --format '{{.Id}}' "$image")
docker run --rm --user "$(id -u):$(id -g)" \
  --mount "type=bind,src=$PWD/input,dst=/input,readonly" \
  --mount "type=bind,src=$PWD/output,dst=/output" \
  --env "IMAGE_ID=$image_id" "$image_id" \
  --input "/input/${1:-transactions_initial.csv}" --output-dir /output --run "${2:-initial}"
