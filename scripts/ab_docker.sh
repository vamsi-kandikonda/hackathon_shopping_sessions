#!/bin/sh
# Alternate runs of two images on a Docker-volume dataset and print sorted
# wall times (ms) for each. Usage: scripts/ab_docker.sh VOLUME IMAGE_A IMAGE_B [ROUNDS]
set -eu
VOLUME="$1"; A="$2"; B="$3"; ROUNDS="${4:-8}"
PLATFORM="${PLATFORM:-linux/arm64}"
run() {
  docker run --rm --platform "$PLATFORM" --network none --read-only \
    --tmpfs /tmp:rw,size=64m --tmpfs /output:rw,size=2g,mode=1777 --tmpfs /scratch:rw,size=1g,mode=1777 \
    --cap-drop ALL --security-opt no-new-privileges --cpus 8 --memory 6g --user 1000:1000 \
    -v "$VOLUME":/data:ro -e NCPU=8 --entrypoint /bin/sh "$1" \
    -c 's=$(date +%s%N); /app/shopping-sessions; e=$(date +%s%N); echo $(( (e-s)/1000000 ))'
}
ta=""; tb=""
i=0
while [ "$i" -lt "$ROUNDS" ]; do
  i=$((i + 1))
  ta="$ta $(run "$A")"
  tb="$tb $(run "$B")"
done
med() { echo "$1" | tr ' ' '\n' | grep -v '^$' | sort -n | awk '{a[NR]=$1} END {print "median " a[int((NR+1)/2)] " ms   all: " ; for (i=1;i<=NR;i++) printf "%s ", a[i]; print ""}'; }
echo "A $A"; med "$ta"
echo "B $B"; med "$tb"
