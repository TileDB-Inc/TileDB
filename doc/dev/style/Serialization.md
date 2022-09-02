---
title: On-disk Serialization and Deserialization
---

## Serialization and Deserialization Guidelines

The status of on-disk serialization is currently in flux. At time of writing, the current guidelines for writing serialize functions for TileDB classes is as follows:


* Only use `sizeof` on fixed-sized types (not variables or types with compiler-dependent size like `bool`).
* Return a TileDB shared pointer to the object in `deserialize` methods. Use exceptions, not `Status` objects, for errors.
* Validate all types either at time of deserialization or in the constructor used for generating the object.
* Add unit tests that check the validations throws the expected errors.


### Validating Enums
For validating enums, consider creating a function that converts from the underlying variable type to the enum with validation. The serialization/deserialization should be to an fixed-size integer of the under-lying type. Consider writing a function with validation to get the actual enum from the integer. For example,
```cpp
inline ArrayType array_type_from_int(uint8_t array_type_enum) {
    auto type = ArrayType(array_type_enum);
    if (type != ArrayType::DENSE && type != ArrayType::SPARSE) {
        throw std::runtime_error(
            "Invalid ArrayType (" + std::to_string(array_type_enum) + ")");
    }
    return type;
}
```
If the enum has many types, use a `switch` statement to validate the types.