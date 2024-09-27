---
title: Tile
---

## Main Structure

Internally tile data is divided into “chunks.” Every tile is at least one chunk. Each tile has the following on-disk format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Num chunks | `uint64_t` | Number of chunks in the tile |
| Chunk 1 | [Chunk](#chunk-format) | First chunk in the tile |
| … | … | … |
| Chunk N | [Chunk](#chunk-format) | N-th chunk in the tile |

## Chunk Format 

A chunk has the following on-disk format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Original length of chunk | `uint32_t` | The original \(unfiltered\) number of bytes of chunk data |
| Filtered chunk length | `uint32_t` | The serialized \(filtered\) number of bytes of chunk data |
| Chunk metadata length | `uint32_t` | Number of bytes in the chunk metadata |
| Chunk metadata | `uint8_t[]` | Chunk metadata bytes |
| Chunk filtered data | `uint8_t[]` | Filtered chunk bytes |

The metadata added to a chunk depends on the sequence of filters in the pipeline used to filter the containing tile.

If a pipeline used to filter tiles is empty \(contains no filters\), the tile is still divided into chunks and serialized according to the above format. In this case there are no chunk metadata bytes \(since there are no filters to add metadata\), and the filtered bytes are the same as original bytes.

The “chunk metadata” before the actual "chunk filtered data" depend on the particular sequence of filters in the pipeline. In the simple case, each filter will simply concatenate its metadata to the chunk metadata region. Because some filters in the pipeline may wish to filter the metadata of previous filters \(e.g. compression, where it is beneficial to compress previous filters’ metadata in addition to the actual chunk data\), the ordering of filters also impacts the metadata that is eventually written to disk.

The “chunk filtered data” bytes contain the final bytes of the chunk after being passed through the entire pipeline. When reading tiles from disk, the filter pipeline is run in the reverse order.

Internally, any filter in a filter pipeline produces two arrays of data as output: a metadata byte array and a filtered data byte array. Additionally, these output byte arrays can be arbitrarily separated into “parts” by any filter. Typically, when a next filter receives the output of the previous filter as its input, it will filter each “part” independently.

### Byteshuffle Filter

The byteshuffle filter does not filter input metadata, and the output data is guaranteed to be the same length as the input data.

The byteshuffle filter produces output metadata in the format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Number of parts | `uint32_t` | Number of data parts |
| Length of part 1 | `uint32_t` | Number of bytes in data part 1 |
| … | … | … |
| Length of part N | `uint32_t` | Number of bytes in data part N |

The byteshuffle filter produces output data in the format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Part 1 | `uint8_t[]` | Byteshuffled data part 1 |
| … | … | … |
| Part N | `uint8_t[]` | Byteshuffled data part N |

### Bitshuffle Filter

The bitshuffle filter does not filter input metadata. It produces output metadata in the format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Number of parts | `uint32_t` | Number of data parts |
| Length of part 1 | `uint32_t` | Number of bytes in data part 1 |
| … | … | … |
| Length of part N | `uint32_t` | Number of bytes in data part N |

The bitshuffle filter produces output data in the format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Part 1 | `uint8_t[]` | Bitshuffled data part 1 |
| … | … | … |
| Part N | `uint8_t[]` | Bitshuffled data part N |

### Bit Width Reduction Filter

The bit width reduction filter does not filter input metadata. It produces output metadata in the format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Length of input | `uint32_t` | Original input number of bytes |
| Number of windows | `uint32_t` | Number of windows in output |
| Window 1 metadata | `WindowMD` | Metadata for window 1 |
| … | … | … |
| Window N metadata | `WindowMD` | Metadata for window N |

The type `WindowMD` has the format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Window value offset | `T` | Offset applied to values in the output window, where `T` is the original datatype of the tile values. |
| Bit width of reduced type | `uint8_t` | Number of bits in the new datatype of the values in the output window |
| Window length | `uint32_t` | Number of bytes in output window data. |

The bit width reduction filter produces output data in the format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Window 1 | `uint8_t[]` | Window 1 data \(possibly-reduced width elements\) |
| … | … | … |
| Window N | `uint8_t[]` | Window N data \(possibly-reduced width elements\) |

> [!NOTE]
> Prior to version 20, the bit width reduction filter had no effect on date and time types.

### Positive Delta Encoding Filter

The positive-delta encoding filter does not filter input metadata. It produces output metadata in the format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Number of windows | `uint32_t` | Number of windows in output |
| Window 1 metadata | `WindowMD` | Metadata for window 1 |
| … | … | … |
| Window N metadata | `WindowMD` | Metadata for window N |

The type `WindowMD` has the format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Window value delta offset | `T` | Offset applied to values in the output window, where `T` is the datatype of the tile values. |
| Window length | `uint32_t` | Number of bytes in output window data. |

The positive-delta encoding filter produces output data in the format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Window 1 | `T[]` | Window 1 delta-encoded data |
| … | … | … |
| Window N | `T[]` | Window N delta-encoded data |

> [!NOTE]
> Prior to version 20, the positive delta encoding filter had no effect on date and time types.

### Compression Filters

The compression filters do filter input metadata. They produce output metadata in the format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Number of metadata parts | `uint32_t` | Number of input metadata parts that were compressed |
| Number of data parts | `uint32_t` | Number of input data parts that were compressed |
| Metadata part 1 | `CompressedPartMD` | Metadata about the first metadata |
| … | … | … |
| Metadata part N | `CompressedPartMD` | Metadata about the nth metadata part |
| Data part 1 | `CompressedPartMD` | Metadata about the first data part |
| … | … | … |
| Data part N | `CompressedPartMD` | Metadata about the nth data part |

The type `CompressedPartMD` has the format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Part original length | `uint32_t` | Input length of the part \(before compression\) |
| Part compressed length | `uint32_t` | Compressed length of the part |

The compression filters then produce output data in the format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Metadata part 0 compressed bytes | `uint8_t[]` | Compressed bytes of the first metadata part |
| … | … | … |
| Metadata part N compressed bytes | `uint8_t[]` | Compressed bytes of the nth metadata part |
| Data part 0 compressed bytes | `uint8_t[]` | Compressed bytes of the first data part |
| … | … | … |
| Data part N compressed bytes | `uint8_t[]` | Compressed bytes of the nth data part |

### Checksum Filters

The filter metadata for `TILEDB_FILTER_CHECKSUM_{MD5,SHA256}` has internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Num metadata checksums | `uint32_t` | Number of checksums computed on input metadata |
| Num data checksums | `uint32_t` | Number of checksums computed on input data |
| Num input bytes for metadata checksum 1 | `uint64_t` | Number of bytes of metadata input to the 1st metadata checksum |
| Metadata checksum 1 | `uint8_t[{16,32}]` (MD5/SHA256) | Checksum produced on first metadata input |
| … | … | … |
| Num input bytes for metadata checksum N | `uint64_t` | Number of bytes of metadata input to the N-th metadata checksum |
| Metadata checksum N | `uint8_t[{16,32}]` (MD5/SHA256) | Checksum produced on N-th metadata input |
| Num input bytes for data checksum 1 | `uint64_t` | Number of bytes of data input to the 1st data checksum |
| Data checksum 1 | `uint8_t[{16,32}]` (MD5/SHA256) | Checksum produced on first data input |
| … | … | … |
| Num input bytes for data checksum N | `uint64_t` | Number of bytes of data input to the N-th data checksum |
| Data checksum N | `uint8_t[{16,32}]` (MD5/SHA256) | Checksum produced on N-th data input |
| Input metadata | `uint8_t[]` | Original input metadata, copied intact |


### Encryption Filters

If the array is **encrypted**, TileDB uses an extra internal filter at the end of the pipeline for AES encryption.

The encryption filter metadata have the following on-disk format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Num metadata parts | `uint32_t` | Number of encrypted metadata parts |
| Num data parts | `uint32_t` | Number of encrypted data parts |
| AES Metadata Part 1 | `AESPart` | Metadata part 1 |
| … | … | … |
| AES Metadata Part N | `AESPart` | Metadata part N |
| AES Data Part 1 | `AESPart` | Data part 1 |
| … | … | … |
| AES Data Part N | `AESPart` | Data part N |

The original metadata is **not** included in the metadata output.

The `AESPart` field has the following on-disk format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Plaintext length | `uint32_t` |  The original unencrypted length of the part |
| Encrypted length | `uint32_t` | The encrypted length of the part |
| IV Bytes | `uint8_t[12]` | AES-256-GCM IV bytes |
| Tag Bytes | `uint8_t[16]` | AES-256-GCM tag bytes |

The data output of the encryption filter is:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Metadata part 1 | `uint8_t[]` |  The encrypted bytes of metadata part 1 |
| … | … | … |
| Metadata part N | `uint8_t[]` |  The encrypted bytes of metadata part N |
| Data part 1 | `uint8_t[]` |  The encrypted bytes of data part 1 |
| … | … | … |
| Metadata part N | `uint8_t[]` |  The encrypted bytes of data part N |

Note that the original input metadata in **not** part of the output.
