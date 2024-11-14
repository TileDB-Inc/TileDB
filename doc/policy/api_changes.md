# Deprecations

## Policy

External C and C++ API deprecations shall be announced (via `TILEDB_DEPRECATED*` annotations) at least two release cycles before removal.

Timeline:
- release X.y: `TILEDB_DEPRECATED_EXPORT` added to function declaration.
- release X.y+1: announce function removal
- release X.y+2: remove function

## Removal Announcements

Function removal shall be announced one release cycle before removal, following at least one release cycle of deprecation.

**Announcements must be included in the release notes for the release preceding the removal.**

### Announcements

- 2.13: All functions deprecated in TileDB <=2.12 will be removed in TileDB 2.15.

## Deprecation Version History

### 2.2.1..2.3.0

- `tiledb_coords`

### 2.3.0..2.4.0

- `tiledb_array_schema_load_with_key`
- `tiledb_query_set_buffer`
- `tiledb_query_set_buffer_var`
- `tiledb_query_set_buffer_nullable`
- `tiledb_query_set_buffer_var_nullable`
- `tiledb_query_get_buffer`
- `tiledb_query_get_buffer_var`
- `tiledb_query_get_buffer_nullable`
- `tiledb_query_get_buffer_var_nullable`
- `tiledb_array_open_at`
- `tiledb_array_open_with_key`
- `tiledb_array_open_at_with_key`
- `tiledb_array_reopen_at`
- `tiledb_array_get_timestamp`
- `tiledb_array_create_with_key`
- `tiledb_array_consolidate_with_key`
- `tiledb_fragment_info_load_with_key`

### 2.6.0..2.7.0

- `tiledb_query_set_subarray`
- `tiledb_query_add_range`
- `tiledb_query_add_range_by_name`
- `tiledb_query_add_range_var`
- `tiledb_query_add_range_var_by_name`
- `tiledb_query_get_range_num`
- `tiledb_query_get_range_num_from_name`
- `tiledb_query_get_range`
- `tiledb_query_get_range_from_name`
- `tiledb_query_get_range_var_size`
- `tiledb_query_get_range_var_size_from_name`
- `tiledb_query_get_range_var`
- `tiledb_query_get_range_var_from_name`

### 2.7.0..2.8.0

- `tiledb_group_create`

### 2.8.0..2.9.0

- `tiledb_array_consolidate_metadata`
- `tiledb_array_consolidate_metadata_with_key`

### 2.13.0..2.14.0

- `tiledb_query_submit_async`

### 2.18.0..2.19.0

- `tiledb_group_get_member_by_index`
- `tiledb_group_get_member_by_name`

### 2.27.0..2.28.0

- `tiledb_filestore_schema_create`
- `tiledb_filestore_uri_import`
- `tiledb_filestore_uri_export`
- `tiledb_filestore_buffer_import`
- `tiledb_filestore_buffer_export`
- `tiledb_filestore_size`
- `tiledb_group_dump_str`
- `tiledb_mime_type_to_str`
- `tiledb_mime_type_from_str`

## Deprecation History Generation

<details>

<summary>Deprecation list was generated using the code below, from the root directory, then hand-edited and verified.</summary>

```julia
import Base.+
+(a::VersionNumber, b::VersionNumber) = VersionNumber(a.major + b.major, a.minor + b.minor, a.patch + b.patch)

versions = [
  v"2.2.1",
  v"2.3.0",
  v"2.4.0",
  v"2.5.0",
  v"2.6.0",
  v"2.7.0",
  v"2.8.0",
  v"2.9.0",
  v"2.10.0",
  v"2.11.0",
  v"2.12.0"
]

data = Dict()

for i in 1:length(versions)-1
  v = versions[i]
  v_next = versions[i+1]
  range = "$v..$v_next"

  data[range] = read(pipeline(`git diff $v..$v_next tiledb/sm/c_api/tiledb.h`, Cmd(`grep -A2 DEPRECATE`, ignorestatus=true)), String)
end

print(data)

open("deprecations.md", "w") do f
  for i in 1:length(versions)-1
    v = versions[i]
    v_next = versions[i+1]
    range = "$v..$v_next"

    println(f, "## $range\n")
    write(f, unescape_string(string(data[range])))
  end
end
```

</details>
