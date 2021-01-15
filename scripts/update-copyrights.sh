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

# This updates the ending year in all occurrences of a copyright string
# in the format:
#
#   @copyright Copyright (c) YYYY TileDB, Inc.
#   @copyright Copyright (c) YYYY-YYYY TileDB, Inc.
#
# Example: if the script is invoked with the year 2020 then the following
# replacements would be made:
#
#   @copyright Copyright (c) YYYY TileDB, Inc.
#   -> @copyright Copyright (c) YYYY-2020 TileDB, Inc.
#   @copyright Copyright (c) YYYY-ZZZZ TileDB, Inc.
#   -> @copyright Copyright (c) YYYY-2020 TileDB, Inc.
#
# Usage example: ./update-copyrights.sh 2020 /path/to/src/dir/

repo="https://github.com/TileDB-Inc/TileDB"

usage() {
    echo "USAGE: $0 <year> <srcdir>"
    echo -n "Recursively updates the copyright year to <year> in all source "
    echo "files in the given directory <srcdir>."
}

if [[ $# -lt 2 ]]; then
    usage
    exit 1
fi

year="$1"
srcdir="$2"

search="Copyright \(c\) ([0-9]{4})(-[0-9]{4})? TileDB"
replace="Copyright (c) \1-$year TileDB"

# Avoid e.g. replacing 'Copyright (c) 2019' with 'Copyright (c) 2019-2019'
except="Copyright \(c\) $year"

# The sed command performs an in-place modification of the files (without
# creating any backups).
find "$srcdir" \( -name "*.h" -or -name "*.cc" -or -name "*.c" \
    -or -name "*.txt" -or -name "*.cmake" -or -name "*.sh" -or -name "*.dox" \) \
    -exec sed -E -e "/$except/! s/$search/$replace/" -i '' "{}" \;