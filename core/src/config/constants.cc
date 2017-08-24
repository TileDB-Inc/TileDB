#include <cfloat>
#include <climits>
#include <cstddef>
#include <cstdint>

#include "constants.h"

namespace tiledb {

namespace constants {

/** The array schema file name. */
const char* array_schema_filename = "__array_schema.tdb";

/** The fragment metadata file name. */
const char* fragment_metadata_filename = "__fragment_metadata.tdb";

/** The default tile capacity. */
const uint64_t capacity = 10000;

/** The size of a variable cell offset. */
const uint64_t cell_var_offset_size = sizeof(size_t);

constexpr uint64_t var_size_expr() {
  return sizeof(size_t) - 1;
}
const uint64_t var_size = var_size_expr();

/** Special name reserved for the coordinates attribute. */
const char* coords = "__coords";

/** The consolidation buffer size. */
const uint64_t consolidation_buffer_size = 10000000;

/** The consolidation filelock name. */
const char* consolidation_filelock_name = "__consolidation_lock";

/** The special value for an empty int32. */
const int empty_int32 = INT_MAX;

/** The special value for an empty int64. */
const int64_t empty_int64 = INT64_MAX;

/** The special value for an empty float32. */
const float empty_float32 = FLT_MAX;

/** The special value for an empty float64. */
const double empty_float64 = DBL_MAX;

/** The special value for an empty char. */
const char empty_char = CHAR_MAX;

/** The special value for an empty int8. */
const int8_t empty_int8 = INT8_MAX;

/** The special value for an empty uint8. */
const uint8_t empty_uint8 = UINT8_MAX;

/** The special value for an empty int16. */
const int16_t empty_int16 = INT16_MAX;

/** The special value for an empty uint16. */
const uint16_t empty_uint16 = UINT16_MAX;

/** The special value for an empty uint32. */
const uint32_t empty_uint32 = UINT32_MAX;

/** The special value for an empty uint64. */
const uint64_t empty_uint64 = UINT64_MAX;

/** The file suffix used in TileDB. */
const char* file_suffix = ".tdb";

/** The fragment file name. */
const char* fragment_filename = "__tiledb_fragment.tdb";

/** The group file name. */
const char* group_filename = "__tiledb_group.tdb";

/** The initial internal buffer size for the case of sparse arrays. */
const uint64_t internal_buffer_size = 10000000;

/** Special name reserved for the metadata key attribute. */
const char* key = "__key";

/** The maximum number of bytes written in a single I/O. */
const uint64_t max_write_bytes = 1500000000;

/** The metadata schema file name. */
const char* metadata_schema_filename = "__metadata_schema.tdb";

/** The maximum name length. */
const unsigned name_max_len = 256;

/** The size of the buffer that holds the sorted cells. */
const uint64_t sorted_buffer_size = 10000000;

/** The size of the buffer that holds the sorted variable cells. */
const uint64_t sorted_buffer_var_size = 10000000;

/** Special value indicating a variable number of elements. */
const int var_num = UINT_MAX;

/** String describing no compression. */
const char* no_compression_str = "NO_COMPRESSION";

/** String describing GZIP. */
const char* gzip_str = "GZIP";

/** String describing ZSTD. */
const char* zstd_str = "ZSTD";

/** String describing LZ4. */
const char* lz4_str = "LZ4";

/** String describing BLOSC. */
const char* blosc_str = "BLOSC_LZ";

/** String describing BLOSC_LZ4. */
const char* blosc_lz4_str = "BLOSC_LZ4";

/** String describing BLOSC_LZ4HC. */
const char* blosc_lz4hc_str = "BLOSC_LZ4HC";

/** String describing BLOSC_SNAPPY. */
const char* blosc_snappy_str = "BLOSC_SNAPPY";

/** String describing BLOSC_ZLIB. */
const char* blosc_zlib_str = "BLOSC_ZLIB";

/** String describing BLOSC_ZSTD. */
const char* blosc_zstd_str = "BLOSC_ZSTD";

/** String describing RLE. */
const char* rle_str = "RLE";

/** String describing BZIP2. */
const char* bzip2_str = "BZIP2";

/** The string representation for type int32. */
const char* int32_str = "INT32";

/** The string representation for type int64. */
const char* int64_str = "INT64";

/** The string representation for type float32. */
const char* float32_str = "FLOAT32";

/** The string representation for type float64. */
const char* float64_str = "FLOAT64";

/** The string representation for type char. */
const char* char_str = "CHAR";

/** The string representation for type int8. */
const char* int8_str = "INT8";

/** The string representation for type uint8. */
const char* uint8_str = "UINT8";

/** The string representation for type int16. */
const char* int16_str = "INT16";

/** The string representation for type uint16. */
const char* uint16_str = "UINT16";

/** The string representation for type uint32. */
const char* uint32_str = "UINT32";

/** The string representation for type uint64. */
const char* uint64_str = "UINT64";

/** The string representation for the dense array type. */
const char* dense_str = "dense";

/** The string representation for the sparse array type. */
const char* sparse_str = "sparse";

/** The string representation for the column-major layout. */
const char* col_major_str = "col-major";

/** The string representation for the row-major layout. */
const char* row_major_str = "row-major";

/** The string representation of null. */
const char* null_str = "null";

}  // namespace constants
}  // namespace tiledb
