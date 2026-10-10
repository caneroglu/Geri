# Starts a throwaway geri-hubd for the demo and exports $H and $AUTH.
# Usage (from the repo root): source demo/env.sh
cargo build -q --release -p geri-hubd || return 1

GERI_DEMO_DIR=$(mktemp -d)
export GERI_AGENTS='[
  {"id":"sat-agent","token":"demo-sat-agent-token-0123","sources":["nasa-sat-01"]}
]'
export GERI_DB=$GERI_DEMO_DIR/geri.db GERI_BLOBS=$GERI_DEMO_DIR/blobs
export GERI_BACKUP_DIR=$GERI_DEMO_DIR/backups GERI_LISTEN=127.0.0.1:8787
./target/release/geri-hubd >"$GERI_DEMO_DIR/hub.log" 2>&1 &
GERI_DEMO_PID=$!
trap 'kill $GERI_DEMO_PID 2>/dev/null; rm -rf "$GERI_DEMO_DIR"' EXIT
sleep 0.5

export H=http://127.0.0.1:8787
export AUTH='Authorization: Bearer demo-sat-agent-token-0123'
cd demo
