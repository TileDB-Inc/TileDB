---
title: Array File Hierarchy
---

An array is a folder with the following structure:

```
my_array                                # array folder
    |_ __schema                         # array schema folder
    |_ __fragments                      # array fragments folder
          |_ <timestamped_name>         # fragment folder
          |_ ...
    |_ __commits                        # array commits folder
          |_ <timestamped_name>.wrt     # fragment write file
          |_ ...
          |_ <timestamped_name>.del     # delete commit file
          |_ ...
          |_ <timestamped_name>.upd     # update commit file
          |_ ...
          |_ <timestamped_name>.vac     # fragment vacuum file
          |_ ...
          |_ <timestamped_name>.con     # consolidated commits file
          |_ ...
          |_ <timestamped_name>.ign     # ignore file for consolidated commits file
    |_ __fragment_meta                  
          |_ <timestamped_name>.meta    # consol. fragment meta file
          |_ ...                  
    |_ __meta                           # array metadata folder

```

A `<timestamped_name>` above has format `__t1_t2_uuid_v`, where 

* `t1` and `t2` are timestamps in milliseconds elapsed since 1970-01-01 00:00:00 +0000 (UTC)
* `uuid` is a unique identifier
* `v` is the format version

Inside the array folder, you can find the following:

* [Array schema](./array_schema.md) folder `__schema`.
* Inside of a fragments folder, any number of [fragment folders](./fragment.md) `<timestamped_name>`.
* Inside of a commit folder, an empty file `<timestamped_name>.wrt` associated with every fragment folder `<timestamped_name>`, where `<timestamped_name>` is common for the folder and the WRT file. This is used to indicate that fragment `<timestamped_name>` has been *committed* (i.e., its write process finished successfully) and it is ready for use by TileDB. If the WRT file does not exist, the corresponding fragment folder is ignored by TileDB during the reads.
* Inside the same commit folder, any number of [delete commit files](./delete_commit_file.md) of the form `<timestamped_name>.del`.
* Inside the same commit folder, any number of [update commit files](./update_commit_file.md) of the form `<timestamped_name>.upd`.
* Inside the same commit folder, any number of [consolidated commits files](./consolidated_commits_file.md) of the form `<timestamped_name>.con`.
* Inside the same commit folder, any number of [ignore files](./ignore_file.md) of the form `<timestamped_name>.ign`.
* Inside of a fragment metadata folder, any number of [consolidated fragment metadata files](./consolidated_fragment_metadata_file.md) of the form `<timestamped_name>.meta`.
* [Array metadata](./metadata.md) folder `__meta`.
