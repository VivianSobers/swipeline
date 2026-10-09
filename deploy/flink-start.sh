#!/bin/sh
set -eu

# Keep the selected job's whole directory: incremental checkpoints can share
# files with earlier checkpoints from that job. Authorization explicitly uses
# NO_CLAIM, making the next job's first checkpoint independent of this directory.
root=${SWIPELINE_FLINK_STATE:-/state}/checkpoints
latest=$(ls -td "$root"/*/chk-*/_metadata 2>/dev/null | head -n 1)
if [ -n "$latest" ]; then
    checkpoint=${latest%/_metadata}
    selected=${checkpoint%/*}
    for job in "$root"/*; do
        [ -d "$job" ] || continue
        [ "$job" = "$selected" ] || rm -rf -- "$job"
    done
    set -- "$@" --fromSavepoint "$checkpoint"
fi
exec "${SWIPELINE_FLINK_ENTRYPOINT:-/docker-entrypoint.sh}" "$@"
