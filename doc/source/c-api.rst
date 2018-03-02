TileDB C API Reference
======================

.. highlight:: c

Types
-----
.. doxygentypedef:: tiledb_config_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_config_iter_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_ctx_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_error_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_attribute_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_array_schema_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_dimension_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_domain_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_query_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_kv_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_kv_item_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_kv_iter_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_vfs_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_vfs_fh_t
    :project: TileDB-C

Return Codes
------------
.. doxygendefine:: TILEDB_OK
    :project: TileDB-C
.. doxygendefine:: TILEDB_ERR
    :project: TileDB-C
.. doxygendefine:: TILEDB_OOM
    :project: TileDB-C

Constants
---------
.. doxygendefine:: TILEDB_COORDS
    :project: TileDB-C
.. doxygendefine:: TILEDB_VAR_NUM
    :project: TileDB-C
.. doxygendefine:: TILEDB_MAX_PATH
    :project: TileDB-C
.. doxygenfunction:: tiledb_coords
    :project: TileDB-C
.. doxygenfunction:: tiledb_var_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_max_path
    :project: TileDB-C

Enumerations
------------
.. doxygenenum:: tiledb_object_t
    :project: TileDB-C
.. doxygenenum:: tiledb_query_type_t
    :project: TileDB-C
.. doxygenenum:: tiledb_query_status_t
    :project: TileDB-C
.. doxygenenum:: tiledb_filesystem_t
    :project: TileDB-C
.. doxygenenum:: tiledb_datatype_t
    :project: TileDB-C
.. doxygenenum:: tiledb_array_type_t
    :project: TileDB-C
.. doxygenenum:: tiledb_layout_t
    :project: TileDB-C
.. doxygenenum:: tiledb_compressor_t
    :project: TileDB-C
.. doxygenenum:: tiledb_walk_order_t
    :project: TileDB-C
.. doxygenenum:: tiledb_vfs_mode_t
    :project: TileDB-C

Context
-------
.. doxygenfunction:: tiledb_ctx_create
    :project: TileDB-C
.. doxygenfunction:: tiledb_ctx_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_ctx_get_config
    :project: TileDB-C
.. doxygenfunction:: tiledb_ctx_get_last_error
    :project: TileDB-C
.. doxygenfunction:: tiledb_ctx_is_supported_fs
    :project: TileDB-C

Config
------
.. doxygenfunction:: tiledb_config_create
    :project: TileDB-C
.. doxygenfunction:: tiledb_config_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_config_set
    :project: TileDB-C
.. doxygenfunction:: tiledb_config_get
    :project: TileDB-C
.. doxygenfunction:: tiledb_config_load_from_file
    :project: TileDB-C
.. doxygenfunction:: tiledb_config_unset
    :project: TileDB-C
.. doxygenfunction:: tiledb_config_save_to_file
    :project: TileDB-C

Config Iterator
---------------
.. doxygenfunction:: tiledb_config_iter_create
    :project: TileDB-C
.. doxygenfunction:: tiledb_config_iter_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_config_iter_here
    :project: TileDB-C
.. doxygenfunction:: tiledb_config_iter_next
    :project: TileDB-C
.. doxygenfunction:: tiledb_config_iter_done
    :project: TileDB-C
.. doxygenfunction:: tiledb_config_iter_reset
    :project: TileDB-C

Error
-----
.. doxygenfunction:: tiledb_error_message
    :project: TileDB-C
.. doxygenfunction:: tiledb_error_free
    :project: TileDB-C


Array
-----
.. doxygenfunction:: tiledb_array_create
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_consolidate
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_non_empty_domain
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_compute_max_read_buffer_sizes
    :project: TileDB-C

Array Schema
------------
.. doxygenfunction:: tiledb_array_schema_create
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_add_attribute
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_set_domain
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_set_capacity
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_set_cell_order
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_set_tile_order
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_set_coords_compressor
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_check
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_load
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_get_array_type
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_get_capacity
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_get_cell_order
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_get_coords_compressor
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_get_offsets_compressor
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_get_domain
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_get_tile_order
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_get_attribute_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_get_attribute_from_index
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_get_attribute_from_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_dump
    :project: TileDB-C

Attribute
---------
.. doxygenfunction:: tiledb_attribute_create
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_set_compressor
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_set_cell_val_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_get_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_get_type
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_get_compressor
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_get_cell_val_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_dump
    :project: TileDB-C

Domain
------
.. doxygenfunction:: tiledb_domain_create
    :project: TileDB-C
.. doxygenfunction:: tiledb_domain_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_domain_get_type
    :project: TileDB-C
.. doxygenfunction:: tiledb_domain_get_rank
    :project: TileDB-C
.. doxygenfunction:: tiledb_domain_add_dimension
    :project: TileDB-C
.. doxygenfunction:: tiledb_domain_get_dimension_from_index
    :project: TileDB-C
.. doxygenfunction:: tiledb_domain_get_dimension_from_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_domain_dump
    :project: TileDB-C

Dimension
---------
.. doxygenfunction:: tiledb_dimension_create
    :project: TileDB-C
.. doxygenfunction:: tiledb_dimension_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_dimension_get_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_dimension_get_type
    :project: TileDB-C
.. doxygenfunction:: tiledb_dimension_get_domain
    :project: TileDB-C
.. doxygenfunction:: tiledb_dimension_get_tile_extent
    :project: TileDB-C
.. doxygenfunction:: tiledb_dimension_dump
    :project: TileDB-C

Query
-----
.. doxygenfunction:: tiledb_query_create
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_set_subarray
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_set_buffers
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_set_layout
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_submit
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_submit_async
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_reset_buffers
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_status
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_attribute_status
    :project: TileDB-C

Group
-----
.. doxygenfunction:: tiledb_group_create
    :project: TileDB-C

Key-value
---------
.. doxygenfunction:: tiledb_kv_create
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_consolidate
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_set_max_buffered_items
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_open
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_close
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_add_item
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_flush
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_get_item
    :project: TileDB-C

Key-value Schema
----------------
.. doxygenfunction:: tiledb_kv_schema_create
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_schema_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_schema_add_attribute
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_schema_check
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_schema_load
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_schema_get_attribute_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_schema_get_attribute_from_index
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_schema_get_attribute_from_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_schema_dump
    :project: TileDB-C

Key-value Item
--------------
.. doxygenfunction:: tiledb_kv_item_create
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_item_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_item_set_key
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_item_set_value
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_item_get_key
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_item_get_value
    :project: TileDB-C

Key-value Iterator
------------------
.. doxygenfunction:: tiledb_kv_iter_create
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_iter_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_iter_here
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_iter_next
    :project: TileDB-C
.. doxygenfunction:: tiledb_kv_iter_done
    :project: TileDB-C

Object Management
-----------------
.. doxygenfunction:: tiledb_object_type
    :project: TileDB-C
.. doxygenfunction:: tiledb_object_remove
    :project: TileDB-C
.. doxygenfunction:: tiledb_object_move
    :project: TileDB-C
.. doxygenfunction:: tiledb_object_walk
    :project: TileDB-C
.. doxygenfunction:: tiledb_object_ls
    :project: TileDB-C

VFS
---
.. doxygenfunction:: tiledb_vfs_create
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_create_bucket
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_remove_bucket
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_empty_bucket
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_is_empty_bucket
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_is_bucket
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_create_dir
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_is_dir
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_remove_dir
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_is_file
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_remove_file
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_file_size
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_move
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_open
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_close
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_read
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_write
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_sync
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_fh_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_fh_is_closed
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_touch
    :project: TileDB-C

URI
---
.. doxygenfunction:: tiledb_uri_to_path
    :project: TileDB-C

Version
-------
.. doxygenfunction:: tiledb_version
    :project: TileDB-C
