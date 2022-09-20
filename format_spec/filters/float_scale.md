---
title: Float Scaling Filter
---

## Float Scaling Filter

The float scale filter converts floating point type data to integer data after first shifting and scaling the data. It is a lossy filter that is often used in combination with other filters that perform well on integer data.
The float scale filter stores its input data (with floating point type) as integer values for more compressed storage.
For example, given `data` as a NumPy float64 array, scale = 2.13, offset = 15.445, and a specified byte width of 4:
  ```
  data = np.random.rand(npts)
  data_b = data.view(np.float64)
  new_data_b = np.zeros(npts, dtype=np.int32) # byte width is 4
  scale = 2.13
  offset = 15.445
  
  for i in range(0, len(data)):
    new_data_b[i] = (data_b[i] - offset) / scale
  ```

  
On read, the float scaling filter will reverse the scale factor and offset, restoring the original floating point data, with a potential loss of precision. Given the previous example:
   ```
   restored_data = np.zeros(npts, dtype=np.float64)
   for i in range(0, len(data)):
     restored_data[i] = (new_data_b[i] * scale) + offset
   ```

# Filter Enum Value

The filter enum value for the float scaling filter is `15` (TILEDB_FILTER_SCALE_FLOAT enum).

# Input and Output Layout

The input and output data layout is identical for the XOR filter.
