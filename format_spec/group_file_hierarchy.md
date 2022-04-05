---
title: Group File Hierarchy
---

A TileDB group is a folder with a single file in it:

```
my_group                       # Group folder
    |_ __tiledb_group.tdb      # Empty group file
```

File `__tiledb_group.tdb` is empty and it is merely used to indicate that `my_group` is a TileDB group.
