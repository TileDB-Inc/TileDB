#!/bin/bash

# This replaces all occurrences of '#xxx' with a Markdown link to the
# pull request of that number.
# Usage example: ./markdown-link-prs.sh HISTORY.md

repo="https://github.com/TileDB-Inc/TileDB"

usage() {
  echo "USAGE: $0 <file.md>"
  echo "Replaces all occurrences of '#xxx' with a Markdown link to the pull request of that number."
}

if [[ $# -lt 1 ]]; then
    usage
    exit 1
fi

filename="$1"

# This performs an in-place modification of the file.
sed -E -e "s|([^\[])#([0-9]+)|\1[#\2](${repo}/pull/\2)|g" -i '' "${filename}"
