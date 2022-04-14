---
title: Group File Hierarchy
---

A TileDB group is a folder with a single file in it:

```
my_group                       # Group folder
    |_ __tiledb_group.tdb      # Empty group file
    |_ __group                 # Group folder
        |_ <timestamped_name>  # Timestamped group file detailing members
    |_ __meta                  # group metadata folder
```

File `__tiledb_group.tdb` is empty and it is merely used to indicate that `my_group` is a TileDB group.

Inside the group folder, you can find the following:

* [Group details](./group.md) folder `__group`.
* [Group metadata](./metadata.md) folder `__meta`.
