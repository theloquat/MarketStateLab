#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
compose() {
  if [ -f .quantos-contest/restore.json ]; then
    docker compose --env-file deploy/contest/secrets.env -f docker-compose.yml -f .quantos-contest/restore.json "$@"
  else
    docker compose --env-file deploy/contest/secrets.env "$@"
  fi
}
case "${1:-status}" in
  setup)
    python3 - <<'PY'
from pathlib import Path
import secrets, os
p=Path('deploy/contest/secrets.env')
if not p.exists():
    fd=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f:
        for key in ['CONTEST_PASSWORD','CONTEST_ROOT_PASSWORD']:
            f.write(key+'='+secrets.token_hex(24)+'\n')
os.chmod(p,0o600)
config=p.read_text()
with p.open('a') as f:
    for k,v in [('CONTEST_UID',os.getuid()),('CONTEST_GID',os.getgid())]:
        if k+'=' not in config:f.write(f'{k}={v}\n')
with p.open('a') as f:
    for k in ['CONTEST_JWT_SECRET','CONTEST_INTERNAL_TOKEN']:
        if k+'=' not in config:f.write(k+'='+secrets.token_hex(32)+'\n')
import sys
sys.path.insert(0,'backend')
from marketlab.access_setup import initialize_access
initialize_access('.quantos-contest')
Path('.quantos-contest').mkdir(exist_ok=True)
print('Local contest configuration ready; credentials are not displayed.')
PY
    ;;
  start)
    "$0" setup
    quantos_expected_build="$(python3 scripts/contest_build_fingerprint.py)"
    quantos_previous_build="$(cat .quantos-contest/build-source.sha256 2>/dev/null || true)"
    if [ "$quantos_expected_build" != "$quantos_previous_build" ] || ! docker image inspect quantos-contest:local quantos-flink:local quantos-gateway:local quantos-minio:2025-10-15-source >/dev/null 2>&1; then "$0" build; fi
    compose up -d --no-build
    ;;
  credentials) cat .quantos-contest/access/credentials.txt ;;
  build)
    "$0" setup
    quantos_before_build="$(python3 scripts/contest_build_fingerprint.py)"
    compose build
    quantos_after_build="$(python3 scripts/contest_build_fingerprint.py)"
    if [ "$quantos_before_build" != "$quantos_after_build" ]; then
      echo 'Build inputs changed during build. Run build again before starting.' >&2
      exit 1
    fi
    printf '%s\n' "$quantos_after_build" > .quantos-contest/build-source.sha256.part
    mv .quantos-contest/build-source.sha256.part .quantos-contest/build-source.sha256
    ;;
  stop) python3 scripts/contest_checkpoint.py; compose stop ;;
  status) compose ps ;;
  logs) compose logs --tail=100 ;;
  local)
    export PYTHONPATH=backend
    exec .venv/bin/python -m uvicorn marketlab.api:app --host 127.0.0.1 --port 18080
    ;;
  *) echo 'Usage: scripts/contest.sh setup|credentials|build|start|stop|status|logs|local'; exit 2 ;;
esac
