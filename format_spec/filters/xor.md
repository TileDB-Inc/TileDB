---
title: XOR Filter
---

The XOR filter applies the XOR operation sequentially to the input data, in chunks of 1-4 bytes, depending on the `sizeof` the attribute's type representation. For example, given `data` as a NumPy int64 array:

  ```
  data = np.random.rand(npts)
  data_b = data.view(np.int64)
  for i in range(1, len(data)):
    data_b[i] = data_b[i] ^ data_b[i-1]
  ```

# Filter Enum Value

The filter enum value for the XOR filter is `16` (TILEDB_FILTER_XOR enum).

# Input and Output Layout

The input and output data layout is identical for the XOR filter.
