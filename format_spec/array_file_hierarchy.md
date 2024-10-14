---
title: Array File Hierarchy
---

An array is a folder with the following structure:

```
my_array                                # array folder
    |_ __schema                         # array schema folder
          |_ <timestamp_name>           # array schema files
          |_ ...
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
    |_ __meta                           # array metadata folder
    |_ __labels                         # dimension label folder
    |_ <timestamped_name>               # legacy fragment folder
          |_ ...
    |_ <timestamped_name>.ok            # legacy fragment write file
    |_ <timestamped_name>.meta          # legacy consolidated fragment meta file
    |_ __array_schema.tdb               # legacy array schema file
```

Inside the array folder, you can find the following:

* Inside of a `__schema` folder, any number of [array schema files](./array_schema.md) [`<timestamped_name>`](./timestamped_name.md).
  * **Note**: the name does _not_ include the format version.
  * _New in version 20_ Inside of the schema folder, an enumerations folder `__enumerations`.
* Inside of a `__meta` folder, any number of [array metadata files](./metadata.md) [`<timestamped_name>`](./timestamped_name.md).
* Inside of a `__fragments` folder, any number of [fragment folders](./fragment.md) [`<timestamped_name>`](./timestamped_name.md).
* _New in version 18_ Inside of a `__labels` folder, additional TileDB arrays storing dimension label data.
* _New in version 12_ Inside of a `__commits` folder:
  * Any number of empty files [`<timestamped_name>`](./timestamped_name.md)`.wrt`, each associated with fragment folder [`<timestamped_name>`](./timestamped_name.md), indicating that the fragment has been *committed* (i.e., its write process finished successfully). If the WRT file does not exist, the corresponding fragment must be ignored when reading the array.
  * Any number of [consolidated commits files](./consolidated_commits_file.md) of the form [`<timestamped_name>`](./timestamped_name.md)`.con`.
  * Any number of [ignore files](./ignore_file.md) of the form [`<timestamped_name>`](./timestamped_name.md)`.ign`.
  * _New in version 16_ Any number of [delete commit files](./delete_commit_file.md) of the form [`<timestamped_name>`](./timestamped_name.md)`.del`.
  * _New in version 16_ Any number of [update commit files](./update_commit_file.md) of the form [`<timestamped_name>`](./timestamped_name.md)`.upd`.
* _New in version 12_ Inside of a `__fragment_meta` folder, any number of [consolidated fragment metadata files](./consolidated_fragment_metadata_file.md) of the form [`<timestamped_name>`](./timestamped_name.md)`.meta`.

> [!NOTE]
> Prior to version 12, fragments, commit files, and consolidated fragment metadata were stored directly in the array folder and the extension of commit files was `.ok` instead of `.wrt`. Implementations must support arrays that contain data in both the old and the new hierarchy at the same time.

> [!NOTE]
> Prior to version 10, the array schema was stored in a single `__array_schema.tdb` file in the array folder. Implementations must support arrays that contain both `__array_schema.tdb` and schemas in the `__schema` folder at the same time. For the purpose of array schema evolution, the timestamp of `__array_schema.tdb` must be considered to be earlier than any schema in the `__schema` folder.

> [!NOTE]
> Prior to version 5, commit files were not written. Fragments of these versions are considered to be committed if their corresponding fragment metadata file exists.
