Quick Experiment for Local Disk Caching
===

This is a quick and dirty approach to utilizing a local disk cache. This has no cache population or management helpers which must be done out of band.

Quick Example
===

Note, you'll need a TileDB-Py built against this branch.

I used this array:

s3://tiledb-johnkerl/for-paul-davis-2023-09-07/foo5/obs

To create the cache, do:

```bash
$ mkdir -p /tmp/test-cache/s3/tiledb-johnkerl/for-paul-davis-2023-09-07/foo5/obs
$ aws s3 sync s3://tiledb-johnkerl/for-paul-davis-2023-09-07/foo5/obs /tmp/test-cache/s3/tiledb-johnkerl/for-paul-davis-2023-09-07/foo5/obs/
$ ./cachify.py /tmp/test-cache # This script is pasted below
$ ./example.py # This script is pasted below
$ export TILEDB_VFS_CACHE_ROOT_DIR=/tmp/test-cache/
$ ./example.py
```

The example script spits out its stats. To verify caching is used, notice that `Context.StorageManager.VFS.read_byte_num` is replaced by `Context.StorageManager.VFS.cache_read_byte_num` in the second run, and if you're lucky and have really terrible internet like me, the query time is a couple orders of magnitude faster.

cachify.py
===

```python
#!/usr/bin/env python3

import tiledb

URI = "s3://tiledb-johnkerl/for-paul-davis-2023-09-07/foo5/obs"

def main():
    with tiledb.open(URI) as arr:
        tiledb.stats_enable()
        print(arr.df[:])
        tiledb.stats_dump()

if __name__ == "__main__":
    main()
```

example.py
===

```python
#!/usr/bin/env python3

import tiledb

URI = "s3://tiledb-johnkerl/for-paul-davis-2023-09-07/foo5/obs"

def main():
    with tiledb.open(URI) as arr:
        tiledb.stats_enable()
        print(arr.df[:])
        tiledb.stats_dump()

if __name__ == "__main__":
    main()
```
