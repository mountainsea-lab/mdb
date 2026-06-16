#!/usr/bin/env bash
set -euo pipefail

fail() {
  echo "deployment artifact validation failed: $*" >&2
  exit 1
}

[[ -f Dockerfile ]] || fail "Dockerfile is missing"
[[ -f .dockerignore ]] || fail ".dockerignore is missing"
[[ -f docker-compose.yml ]] || fail "docker-compose.yml is missing"
[[ -f config/docker.env.example ]] || fail "config/docker.env.example is missing"
[[ -f README.md ]] || fail "README.md is missing"

grep -q '^rust-toolchain.toml$' .dockerignore || fail ".dockerignore must exclude rust-toolchain.toml to avoid rustup component downloads in Docker builds"

grep -q 'cargo build --release --bin fdc_server' Dockerfile || fail "Dockerfile must build fdc_server release binary"
grep -q 'FROM .*bookworm.* AS runtime' Dockerfile || fail "Dockerfile must use a Debian bookworm runtime stage"
grep -q 'CMD \["/usr/local/bin/fdc_server"\]' Dockerfile || fail "Dockerfile must run /usr/local/bin/fdc_server"
grep -q 'HEALTHCHECK' Dockerfile || fail "Dockerfile must include HEALTHCHECK"

grep -q '^services:' docker-compose.yml || fail "docker-compose.yml must define services"
grep -q 'fdc-server:' docker-compose.yml || fail "docker-compose.yml must define fdc-server service"
grep -q '18080:18080' docker-compose.yml || fail "docker-compose.yml must publish port 18080"
grep -q 'config/docker.env.example' docker-compose.yml || fail "docker-compose.yml must reference config/docker.env.example"
grep -q './data/fdc-market-data:/app/var/fdc-market-data' docker-compose.yml || fail "docker-compose.yml must persist market-data storage"
grep -q 'http://localhost:18080/health' docker-compose.yml || fail "docker-compose.yml must healthcheck /health"

grep -q '^FDC_SERVER_ADDR=0.0.0.0:18080$' config/docker.env.example || fail "docker env must bind to 0.0.0.0:18080"
grep -q '^FDC_MARKET_DATA_STORAGE_L2_REDB_PATH=./var/fdc-market-data/l2.redb$' config/docker.env.example || fail "docker env must set l2 path under /app/var"
grep -q '^FDC_MARKET_DATA_STORAGE_L3_DUCKDB_PATH=./var/fdc-market-data/l3.duckdb$' config/docker.env.example || fail "docker env must set l3 path under /app/var"
grep -q '^FDC_MARKET_DATA_STORAGE_L4_ROCKSDB_PATH=./var/fdc-market-data/l4-rocksdb$' config/docker.env.example || fail "docker env must set l4 path under /app/var"

grep -q '### Docker 镜像构建与 Compose 部署' README.md || fail "README must include Docker deployment section"
grep -q 'docker build -t fdc-server:local .' README.md || fail "README must document docker build"
grep -q 'docker compose up -d --build' README.md || fail "README must document compose startup"
grep -q 'curl http://127.0.0.1:18080/health' README.md || fail "README must document health check"

echo "deployment artifact validation passed"
