#!/bin/sh
# Time the image on a dataset stored in a Docker volume (Linux page cache, no
# macOS file sharing in the way), with the scorer's container flags.
# Usage: scripts/bench_docker.sh VOLUME [RUNS]
# Optional env: SS_SERIAL=1 (per-phase CPU), SS_EXP=<experiment>, CPUS, MEM, PLATFORM.
set -eu
VOLUME="$1"
RUNS="${2:-3}"
IMAGE="${IMAGE:-shopping-sessions:latest}"
CPUS="${CPUS:-8}"
MEM="${MEM:-6g}"
PLATFORM="${PLATFORM:-linux/arm64}"
EXTRA=""
[ -n "${SS_SERIAL:-}" ] && EXTRA="$EXTRA -e SS_SERIAL=1"
[ -n "${SS_EXP:-}" ] && EXTRA="$EXTRA -e SS_EXP=$SS_EXP"
for kv in ${DOCKER_ENV:-}; do EXTRA="$EXTRA -e $kv"; done
i=0
while [ "$i" -lt "$RUNS" ]; do
  i=$((i + 1))
  # shellcheck disable=SC2086
  docker run --rm --platform "$PLATFORM" \
    --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
    --tmpfs /output:rw,size=2g,mode=1777 --tmpfs /scratch:rw,size=1g,mode=1777 \
    --cap-drop ALL --security-opt no-new-privileges \
    --cpus "$CPUS" --memory "$MEM" --memory-swap "$MEM" --user 1000:1000 \
    -v "$VOLUME":/data:ro \
    -e NCPU="${CPUS%.*}" -e SS_TIMING=1 $EXTRA \
    --entrypoint /bin/sh "$IMAGE" -c 'start=$(date +%s%N); /app/shopping-sessions; end=$(date +%s%N); echo "  run wall $(( (end - start) / 1000000 )) ms"' 2>&1 \
    || true
done
