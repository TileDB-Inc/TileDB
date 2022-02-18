# Error handling

## Status

Currently, TileDB primarily uses `Status` objects for handlinge errors. The `Status` class is defined in `tiledb/common/status.h`. A number of methods exist to create status messages local to a specific area of the code. To generate a new Status with clear status (no error), use the static method `Ok`.

For example, a snippet of code that is checking for an error related to the Context might look like the following:

```c++
#include "tiledb/common/status.h"

using namespace = tiledb::common;

...

    if (failure_condition)
        return Status_ContextError("Cannot do requested thing");
    return Status::Ok(); 
```


## Exception

Not yet implemented

## Style guide for error messages

TBD