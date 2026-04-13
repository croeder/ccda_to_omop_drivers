#!/usr/bin/env bash
# sync_data.sh — copy CCDA XML input data to the NFS input share on pi4.
#
# Usage:
#   ./sync_data.sh [source_dir]
#
#   source_dir defaults to CCDA-data/xml_load_test (large dataset).
#   Use the small test set with:
#     ./sync_data.sh ../CCDA_OMOP_Conversion_Package/resources

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"

SOURCE="${1:-$REPO_ROOT/CCDA-data/xml_load_test}"
NFS_SERVER="${NFS_SERVER:-10.0.1.201}"
SSH_USER="${SSH_USER:-croeder}"
DEST="${SSH_USER}@${NFS_SERVER}:/srv/nfs/input"

if [[ ! -d "$SOURCE" ]]; then
    echo "Error: source directory not found: $SOURCE" >&2
    exit 1
fi

xml_count=$(find "$SOURCE" -maxdepth 1 -name "*.xml" | wc -l | tr -d ' ')
echo "Source:      $SOURCE ($xml_count XML files)"
echo "Destination: $DEST"
echo ""

rsync -az --delete --progress \
    --include="*.xml" --exclude="*" \
    "$SOURCE/" "$DEST/"

echo ""
echo "Sync complete."
