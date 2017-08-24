#ifndef TILEDB_CONSTANTS_H
#define TILEDB_CONSTANTS_H

namespace tiledb {
namespace constants {

/** The array schema file name. */
extern const char* array_schema_filename;

/** The fragment metadata file name. */
extern const char* fragment_metadata_filename;

/** The default tile capacity. */
extern const uint64_t capacity;

/** The size of a variable cell offset. */
extern const uint64_t cell_var_offset_size;

/** Special name reserved for the coordinates attribute. */
extern const char* coords;

/** The consolidation buffer size. */
extern const uint64_t consolidation_buffer_size;

/** The consolidation filelock name. */
extern const char* consolidation_filelock_name;

/** The special value for an empty int32. */
extern const int empty_int32;

/** The special value for an empty int64. */
extern const int64_t empty_int64;

/** The special value for an empty float32. */
extern const float empty_float32;

/** The special value for an empty float64. */
extern const double empty_float64;

/** The special value for an empty char. */
extern const char empty_char;

/** The special value for an empty int8. */
extern const int8_t empty_int8;

/** The special value for an empty uint8. */
extern const uint8_t empty_uint8;

/** The special value for an empty int16. */
extern const int16_t empty_int16;

/** The special value for an empty uint16. */
extern const uint16_t empty_uint16;

/** The special value for an empty uint32. */
extern const uint32_t empty_uint32;

/** The special value for an empty uint64. */
extern const uint64_t empty_uint64;

/** The file suffix used in TileDB. */
extern const char* file_suffix;

/** The fragment file name. */
extern const char* fragment_filename;

/** The group file name. */
extern const char* group_filename;

/** The initial internal buffer size for the case of sparse arrays. */
extern const uint64_t internal_buffer_size;

/** Special name reserved for the metadata key attribute. */
extern const char* key;

/** The maximum number of bytes written in a single I/O. */
extern const uint64_t max_write_bytes;

/** The metadata schema file name. */
extern const char* metadata_schema_filename;

/** The maximum name length. */
extern const unsigned name_max_len;

/** The size of the buffer that holds the sorted cells. */
extern const uint64_t sorted_buffer_size;

/** The size of the buffer that holds the sorted variable cells. */
extern const uint64_t sorted_buffer_var_size;

/** Special value indicating a variable number of elements. */
extern const int var_num;

/** Special value indicating a variable size. */
extern const uint64_t var_size;

/** String describing no compression. */
extern const char* no_compression_str;

/** String describing GZIP. */
extern const char* gzip_str;

/** String describing ZSTD. */
extern const char* zstd_str;

/** String describing LZ4. */
extern const char* lz4_str;

/** String describing BLOSC. */
extern const char* blosc_str;

/** String describing BLOSC_LZ4. */
extern const char* blosc_lz4_str;

/** String describing BLOSC_LZ4HC. */
extern const char* blosc_lz4hc_str;

/** String describing BLOSC_SNAPPY. */
extern const char* blosc_snappy_str;

/** String describing BLOSC_ZLIB. */
extern const char* blosc_zlib_str;

/** String describing BLOSC_ZSTD. */
extern const char* blosc_zstd_str;

/** String describing RLE. */
extern const char* rle_str;

/** String describing BZIP2. */
extern const char* bzip2_str;

/** The string representation for type int32. */
extern const char* int32_str;

/** The string representation for type int64. */
extern const char* int64_str;

/** The string representation for type float32. */
extern const char* float32_str;

/** The string representation for type float64. */
extern const char* float64_str;

/** The string representation for type char. */
extern const char* char_str;

/** The string representation for type int8. */
extern const char* int8_str;

/** The string representation for type uint8. */
extern const char* uint8_str;

/** The string representation for type int16. */
extern const char* int16_str;

/** The string representation for type uint16. */
extern const char* uint16_str;

/** The string representation for type uint32. */
extern const char* uint32_str;

/** The string representation for type uint64. */
extern const char* uint64_str;

/** The string representation for the dense array type. */
extern const char* dense_str;

/** The string representation for the sparse array type. */
extern const char* sparse_str;

/** The string representation for the column-major layout. */
extern const char* col_major_str;

/** The string representation for the row-major layout. */
extern const char* row_major_str;

/** The string representation of null. */
extern const char* null_str;
}  // namespace constants
}  // namespace tiledb
#endif  // TILEDB_CONSTANTS_H
