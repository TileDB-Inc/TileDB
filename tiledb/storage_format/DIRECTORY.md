# Storage Format

TileDB arrays are stored within a directory structure on some object store or file system. The information of an array is stored in three different forms. The data of the database is within files, of course, but there's also necessary information in other places.

* **Directory.** Certain files and other directories must be present or may be present within the content of a directory.
* **File Name.** File names are an encoded set of fields.
* **File.** Each type of file has its own data layout.

Each of these entities may be regarded as having syntax and semantics: what a valid entity is and what it means. The scope of this directory is to support these two aspects of array storage.

## Out of scope
Here's a partial list of concerns that are out of scope:

* **Storage access.** Nothing in this directory has direct access to an object store or a file system. A user of these classes must provide the results of a storage operation to this code for parsing and interpretation.
* **State maintenance.** This code has the notion of an "array in a directory", but it does not provide any notion of "the current state" of an array in storage. The concept here is that of an array a data type, not as a variable of that data type.

## Useful Information
The following is the list of Datatypes that are **not** supported by the Dimension class.
Please note that this information is repeated in the C API, C++ API, and the Dimension class.

* TILEDB_CHAR
* TILEDB_BLOB
* TILEDB_GEOM_WKB
* TILEDB_GEOM_WKT
* TILEDB_BOOL
* TILEDB_STRING_UTF8
* TILEDB_STRING_UTF16
* TILEDB_STRING_UTF32
* TILEDB_STRING_UCS2
* TILEDB_STRING_UCS4
* TILEDB_ANY