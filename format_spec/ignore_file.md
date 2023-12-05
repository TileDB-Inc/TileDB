---
title: Ignore File
---

A ignore file has name [`<timestamped_name>`](./timestamped_name.md)`.ign` and is located in the array commit or metadata folder:

```
my_array                           # array folder
   |_ ....
   |_ __commits                    # array commit folder
         |_ <timestamped_name>.ign      # ignore file
```

The ignore file is a simple text file where each line contains a URI string. The URI is the relative URI based on the top level array URI.

Ignored files are used to specify commit files that should be ignored. The timestamps for the file is used to identify the timestamp range that is included inside of the file (with the smaller/bigger timestamp from contained elements). That way if an array is opened at a time that doesn't intersect the timestamp range of the file, it can be omitted. All ignore files that intersect the array open start/end times will be loaded when the array is opened. There are a few situations where those files are required, all linked to [consolidated commits files](./consolidated_commits_file.md). One of them is when fragments are vacuumed after consolidation. If any fragments were included a consolidated commits file, they now should be added to an ignore file so that the system doesn't try to load that fragments. Another is when a fragment that is included in a consolidated commits file is deleted, it needs to be added to an ignore file so that the array can still be opened.

Once a new consolidated commits file get generated, it will not include any of the fragments that were deleted or vacuumed. At that point, the ignore files and the other consolidated commits files can be vacuumed.

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| URI 1 followed by a new line character | `uint8_t[]` | URI 1 to be ignored |
| … | … | … |
| URI N followed by a new line character | `uint8_t[]` | URI N to be ignored |
