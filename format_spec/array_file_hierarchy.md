# Array File Hierarchy

An array is a folder with the following structure:

```
my_array                          # array folder
    |_ __schema                   # array schema folder
    |_ <timestamped_name>         # fragment folder
    |_ <timestamped_name>.ok      # fragment ok file
    |_ ...
    |_ <timestamped_name>.vac     # fragment vacuum file
    |_ ...                  
    |_ <timestamped_name>.meta    # consol. fragment meta file
    |_ ...                  
    |_ __meta                     # array metadata folder

```

A `<timestamped_name>` above has format `__t1_t2_uuid_v`, where 
* `t1` and `t2` are timestamps in milliseconds elapsed since 1970-01-01 00:00:00 +0000 (UTC)
* `uuid` is a unique identifier
* `v` is the format version

Inside the array folder, you can find the following:

* [Array schema](./array_schema.md) folder `__schema`.
* Any number of [fragment folders](./fragment.md) `<timestamped_name>`.
* An empty file `<timestamped_name>.ok` associated with every fragment folder `<timestamped_name>`, where `<timestamped_name>` is common for the folder and the OK file. This is used to indicate that fragment `<timestamped_name>` has been *committed* (i.e., its write process finished successfully) and it is ready for use by TileDB. If the OK file does not exist, the corresponding fragment folder is ignored by TileDB during the reads.
* Any number of [vacuum files](./vacuum_file.md) of the form `<timestamped_name>.vac`.
* Any number of [consolidated fragment metadata files](./consolidated_fragment_metadata_file.md) of the form `<timestamped_name>.meta`.
* [Array metadata](./array_metadata.md) folder `__meta`.