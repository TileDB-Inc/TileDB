# Dimension Labels

## Main Structure

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Number of labels | `uint32_t` | Number of dimension labels |
| Label 1 | [Label](#label) | First label |
| …  | … | … |
| Label N | [Label](#label) | Nth label |

## Label

The label has internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Label type | `uint8_t` | Type of label \(e.g. `TILEDB_LABEL_UNIFORM`\) |
| Label name length | `uint32_t` | Number of characters in label name |
| Label name | `char[]` | Label name character array |
| Label datatype | `uint8_t` | Datatype of the coordinate values |
| Label cell val num | `uint32_t` | Number of coordinate values per cell. For variable-length labels, this is `std::numeric_limits<uint32_t>::max()` |
| Label domain size | `uint64_t[]` | The domain size in bytes |
| Label domain | `uint8_t[]` | Byte array of length equal to domain size above, storing the min, max values of the label. |
| Label metadata size | `uint32_t` | Number of bytes in label metadata — may be 0. |
| Label metadata | [Label Metadata](#label-metadata) | Label metadata, specific to each label. |

## Label Metadata

The label metadata are the parameters that define the label.

### Uniform Label

`TILEDB_LABEL_UNIFORM` does not serialize any additional options.