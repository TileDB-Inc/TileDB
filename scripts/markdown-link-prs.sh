#!/bin/bash

#
# The MIT License (MIT)
#
# Copyright (c) 2019-2021 TileDB, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

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
