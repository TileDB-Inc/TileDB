# TileDB test tools

This directory contains the source for the test tools.

**Note**: These tools should currently be considered experimental, and they are not built by default with TileDB.

## Building

To build the `main` test executable, first configure TileDB with the CMake variable `-DTILEDB_TEST_TOOLS=ON`. Then use the `test_tools` make target e.g.

```bash
$ cmake -DTILEDB_TEST_TOOLS=ON ..
$ make && make -C tiledb test_tools
```

This will produce the binary `tiledb/test_tools/main`.

