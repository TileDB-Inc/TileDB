---
title: Float Scaling Filter
---

## Float Scaling Filter

The float scale filter converts floating point type data to integer data after first shifting and scaling the data. It is a lossy filter that is often used in combination with other filters that perform well on integer data.
The float scale filter stores its input data (with floating point type) as integer values for more compressed storage.
For example, given `data` as a NumPy float64 array, with scale/offset parameters, and a specified byte width of 4:
  ```
  data = np.random.rand(npts)
  data_b = data.view(np.float64)
  new_data_b = np.zeros(npts, dtype=np.int32) # byte width is 4
  scale, offset

  for i in range(0, len(data)):
    # round works like <cmath>'s round
    new_data_b[i] = round((data_b[i] - offset) / scale)
  ```


On read, the float scaling filter will reverse the scale factor and offset, restoring the original floating point data, with a potential loss of precision. Given the previous example:
   ```
   restored_data = np.zeros(npts, dtype=np.float64)
   for i in range(0, len(data)):
     restored_data[i] = (new_data_b[i] * scale) + offset
   ```

Here's a small example of the float scaling filter's behavior on write, then read. Given the parameters `scale = 0.25`, `offset = 10.0 `, and `byte_size = 2`, this is how the data is stored and retrieved.
```
  input_data (double) = [10.0, 10.2500, 10.7540, 11.0001]
  stored_data (16 bit integer) = [0, 1, 3, 4]
  output_data (double) = [10.0, 10.2500, 10.7500, 11.0]
```

### Filter Enum Value

The filter enum value for the float scaling filter is `15` (TILEDB_FILTER_SCALE_FLOAT enum).

### Input and Output Layout

The input data layout will be an array of floating point numbers. The output data layout will be an array of integer numbers with the pre-specified byte width.
