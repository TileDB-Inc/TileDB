---
title: Dictionary Encoding Filter
---

The Dictionary Encoding filter compresses losslessly string data by creating a set of unique strings (called the `dictionary`) from the input data and substituting the string data on disk with their position in the dictionary.
As an example in pseudocode:

  ```
  input_data = ["HG543232", "HG543232", "HG543232", "HG54", "HG54", "A", "HG543232", "HG54"]
  # apply dictionary encoding ->
  dictionary = ["HG543232", "HG54", "A"]
  output_data = [0, 0, 0, 1, 1, 2, 0, 1]
  ```

The dictionary encoding filter is supported only for variable-sized strings and must be the first filter in the filter pipeline.

# Filter Enum Value

The filter enum value for the Dictionary Encoding filter is `14` (`TILEDB_FILTER_DICTIONARY` enum).

# Input and Output Layout

* Input is an array of strings `[str1|...|strN]`.
* Output is an array of integers `[num1|...|numN]`, that are actually the indices in the dictionary of the strings that appeared in input, in the order that they appeared. The integer datasize depends on the input and is chosen between `uint8_t`, `uint16_t`, `uint32_t` or `uint64_t` as the minimum datasize that can hold the size of the dictionary, aka the number of unique strings in input and stored in output metadata.
* Output metadata are in the form: `[output_datasize|dict_string_datasize|dictionary]` where `dictionary` is in the form: `[num_of_strings|size_str1|str1|...|size_strN|strN]`. `output_datasize` refers to the chosen datasize of the integers in output while `dict_string_datasize` refers to the minimum integer size that can hold the maximum length string in input and is also chosen between `uint8_t`, `uint16_t`, `uint32_t` or `uint64_t`.

All the above integers are stored in big-endian format.

Because the dictionary encoding filter works on variable-sized cells of data, it filters the cell data and offsets combined and its output gets stored in the variable-sized data file, after applying any subsequent filters. The fixed-sized data file does not contain any data.

> [!NOTE]
> Prior to version 13 for ASCII strings and version 17 for UTF-8 strings, the offsets buffers was separately filtered as well due to an oversight. Accessing the cell offsets only is generally not useful to implementations.
