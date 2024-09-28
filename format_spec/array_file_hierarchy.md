---
title: Array File Hierarchy
---

An array is a folder with the following structure:

```
my_array                                # array folder
    |_ __schema                         # array schema folder
          |_ __enumerations             # array enumerations folder
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
    |_ __fragment_meta                  # consolidated fragment metadata folder
          |_ <timestamped_name>.meta    # consolidated fragment meta file
          |_ ...
    |_ <timestamped_name>               # legacy fragment folder
          |_ ...
    |_ <timestamped_name>.ok            # legacy fragment write file
    |_ <timestamped_name>.meta          # legacy consolidated fragment meta file
    |_ __meta                           # array metadata folder
    |_ __labels                         # dimension label folder
```

Inside the array folder, you can find the following:

* [Array schema](./array_schema.md) folder `__schema`.
  * _New in version 20_ Inside of the schema folder, an enumerations folder `__enumerations`.
* Inside of a `__fragments` folder, any number of [fragment folders](./fragment.md) [`<timestamped_name>`](./timestamped_name.md).
* _New in version 12_ Inside of a `__commits` folder, an empty file [`<timestamped_name>`](./timestamped_name.md)`.wrt` associated with every fragment folder [`<timestamped_name>`](./timestamped_name.md), where [`<timestamped_name>`](./timestamped_name.md) is common for the folder and the WRT file. This is used to indicate that fragment [`<timestamped_name>`](./timestamped_name.md) has been *committed* (i.e., its write process finished successfully) and it is ready for use by TileDB. If the WRT file does not exist, the corresponding fragment folder is ignored by TileDB during the reads.
  * _New in version 16_ Inside the same commits folder, any number of [delete commit files](./delete_commit_file.md) of the form [`<timestamped_name>`](./timestamped_name.md)`.del`.
  * _New in version 16_ Inside the same commits folder, any number of [update commit files](./update_commit_file.md) of the form [`<timestamped_name>`](./timestamped_name.md)`.upd`.
  * Inside the same commits folder, any number of [consolidated commits files](./consolidated_commits_file.md) of the form [`<timestamped_name>`](./timestamped_name.md)`.con`.
  * Inside the same commits folder, any number of [ignore files](./ignore_file.md) of the form [`<timestamped_name>`](./timestamped_name.md)`.ign`.
* _New in version 12_ Inside of a fragment metadata folder, any number of [consolidated fragment metadata files](./consolidated_fragment_metadata_file.md) of the form [`<timestamped_name>`](./timestamped_name.md)`.meta`.
* [Array metadata](./metadata.md) folder `__meta`.
* _New in version 18_ Inside of a `__labels` folder, additional TileDB arrays storing dimension label data.

> [!NOTE]
> Prior to version 12, fragments, commit files, and consolidated fragment metadata were stored directly in the array folder and the extension of commit files was `.ok` instead of `.wrt`. Implementations must support arrays that contain data in both the old and the new hierarchy at the same time.
