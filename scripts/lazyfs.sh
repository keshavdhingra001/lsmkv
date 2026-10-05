#!/usr/bin/env bash
# Power-cut test on a real filesystem (DESIGN.md D31): builds LazyFS, mounts
# it, and runs the kill -9 harness there, dropping every unsynced byte after
# each kill. Needs Linux with FUSE 3 (apt install fuse3 libfuse3-dev cmake g++).
#
#   scripts/lazyfs.sh [rounds]        default 50
set -euo pipefail

ROUNDS="${1:-50}"
WORK="${LAZYFS_WORK:-$PWD/target/lazyfs}"
SRC="$WORK/src"
MNT="$WORK/mnt"
ROOT="$WORK/root"
FIFO="$WORK/faults.fifo"
DONE="$WORK/faults-done.fifo"

mkdir -p "$WORK" "$MNT" "$ROOT"
if [ ! -x "$SRC/lazyfs/build/lazyfs" ]; then
    rm -rf "$SRC"
    git clone --depth 1 https://github.com/dsrhaslab/lazyfs "$SRC"
    # Fetch spdlog with git rather than as a GitHub archive download, which
    # some sandboxed networks block while still allowing git.
    sed -i 's|URL *https://github.com/gabime/spdlog/archive/v1.10.0.tar.gz|GIT_REPOSITORY https://github.com/gabime/spdlog.git GIT_TAG v1.10.0|' \
        "$SRC/libs/libpcache/CMakeLists.txt"
    (cd "$SRC/libs/libpcache" && ./build.sh)
    (cd "$SRC/lazyfs" && ./build.sh)
fi

cat > "$WORK/config.toml" <<TOML
[faults]
fifo_path="$FIFO"
fifo_path_completed="$DONE"

[cache]
apply_eviction=false

[cache.simple]
custom_size="1GB"
blocks_per_page=1

[filesystem]
log_all_operations=false
logfile="$WORK/lazyfs.log"
TOML

cleanup() { fusermount3 -u "$MNT" 2>/dev/null || true; }
trap cleanup EXIT
cleanup
rm -f "$FIFO" "$DONE"
(cd "$SRC/lazyfs" && ./scripts/mount-lazyfs.sh -c "$WORK/config.toml" -m "$MNT" -r "$ROOT" -s)
for _ in $(seq 50); do [ -p "$FIFO" ] && break; sleep 0.2; done
[ -p "$FIFO" ] || { echo "LazyFS didn't start; see $WORK/lazyfs.log"; exit 1; }

LSMKV_LAZYFS_DIR="$MNT" LSMKV_LAZYFS_FIFO="$FIFO" LSMKV_LAZYFS_DONE="$DONE" \
LSMKV_CRASH_ROUNDS="$ROUNDS" \
    cargo test --release --test kill9 lazyfs_power_cuts -- --ignored --nocapture
