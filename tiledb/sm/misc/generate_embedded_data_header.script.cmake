#
# tiledb/sm/misc/generate_embedded_data_header.script.cmake
#
#
# The MIT License
#
# Copyright (c) 2024 TileDB, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

if (NOT INPUT_FILE)
    message(FATAL_ERROR "Must specify INPUT_FILE")
endif()

if (NOT OUTPUT_FILE)
    message(FATAL_ERROR "Must specify OUTPUT_FILE")
endif()

cmake_path(GET INPUT_FILE FILENAME INPUT_FILENAME)
cmake_path(REPLACE_FILENAME OUTPUT_FILE "${INPUT_FILENAME}" OUTPUT_VARIABLE compressed_file)
set(compressed_file "${compressed_file}.zst")

string(MAKE_C_IDENTIFIER ${INPUT_FILENAME} INPUT_VARIABLE)

message(DEBUG "Compressing ${INPUT_FILE} to ${compressed_file}")
file(ARCHIVE_CREATE OUTPUT "${compressed_file}" PATHS ${INPUT_FILE} FORMAT raw COMPRESSION Zstd
    # Level 12 was found to have the best balance between compression ratio and speed
    # but is available in CMake 3.26+.
    COMPRESSION_LEVEL 9
)
file(SIZE ${compressed_file} COMPRESSED_SIZE)
message(DEBUG "Compressed size: ${COMPRESSED_SIZE} bytes")

message(DEBUG "Reading compressed data from ${compressed_file}")
file(READ "${compressed_file}" compressed_data HEX)
file(REMOVE "${compressed_file}")

message(DEBUG "Writing embedded data to ${OUTPUT_FILE}")

# Split the data into lines of 128 byte literals.
set(MAX_CHARACTERS_PER_LINE 128)
string(REPEAT "[0-9a-f][0-9a-f]" ${MAX_CHARACTERS_PER_LINE} characters_per_line)
string(REGEX REPLACE "(${characters_per_line})" "\\1\n" compressed_data "${compressed_data}")

# Convert the hex characters to C-style hex numbers.
string(REGEX REPLACE "([0-9a-f][0-9a-f])" "0x\\1, " compressed_data "${compressed_data}")

file(SIZE ${INPUT_FILE} DECOMPRESSED_SIZE)

file(WRITE ${OUTPUT_FILE}
    "static const unsigned char ${INPUT_VARIABLE}_compressed_bytes[] = {\n"
    ${compressed_data} "\n"
    "};\n"
    "constexpr size_t ${INPUT_VARIABLE}_compressed_size = sizeof(${INPUT_VARIABLE}_compressed_bytes);\n"
    "constexpr size_t ${INPUT_VARIABLE}_decompressed_size = ${DECOMPRESSED_SIZE};\n")
