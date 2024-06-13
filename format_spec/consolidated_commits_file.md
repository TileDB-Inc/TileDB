---
title: Consolidated Commits File
---

A consolidated commits file has name [`<timestamped_name>`](./timestamped_name.md)`.con` and is located here:

```
my_array                              # array folder
   |_ ....
   |_ __commits                       # array commits folder
         |_ <timestamped_name>.con    # consolidated commits file
         |_ ...
```

There may be multiple such files in the array commits folder. Each consolidated commits file combines a list of fragments commits, delete or update commits that will be considered together when opening an array. They are timestamped (with the smaller/bigger timestamp from contained elements) so that they might be filtered if the array open start/end time don't intersect anything inside of the file, but any file where there is any intersection will be loaded. There are two situations when these files are added. The first one is when a user decides to improve the open performance of their array (by reducing the listing size) through commits consolidation. The second one is when a user has specified a maximum fragment size for consolidation and multiple fragments needed to be committed at once, this file format will be used so that an atomic file system operation can be performed to do so.

When fragments that are included inside of a consolidated file are removed, either through vacuuming or other fragment deletion, an [ignore file](./ignore_file.md) needs to be written so that those commits file can be ignored when opening an array and not cause unnecessary file system operations.

There are three types of objects that can be in the file: commits, deletes or updates. For each of those, the first field is the URI of the object, which determines the subsequent serialized content of the data block:
- a URI ending with `.ok` or `.wrt` is a commit object containing only the fragment URI
- a URI ending with `.del` is a delete object containing the serialized delete condition
- a URI ending with `.upd` is an update object containing the serialized update condition

### Fragment commit:

| **Field** | **Type** | **Extension** | **Description** |
| :--- | :--- | :--- | :--- |
| Commit URI followed by a new line character | `uint8_t[]` | .ok, .wrt | URI |

### Delete commit (see [delete commit file](./delete_commit_file.md) for the format of the serialized delete condition):

| **Field** | **Type** | **Extension** | **Description** |
| :--- | :--- | :--- | :--- |
| URI followed by a new line character | `uint8_t[]` | .del | URI |
| Serialized delete condition tile size | `uint64_t` | | Delete condition tile size |
| Serialized delete condition tile | `uint8_t[]` | | Delete condition tile |

### Update commit (see [update commit file](./update_commit_file.md) for the format of the serialized update condition):

| **Field** | **Type** | **Extension** | **Description** |
| :--- | :--- | :--- | :--- |
| URI followed by a new line character | `uint8_t[]` | .upd | URI |
| Serialized update condition tile size | `uint64_t` | | Update condition tile size |
| Serialized update condition tile | `uint8_t[]` | | Update condition tile |
