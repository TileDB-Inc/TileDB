TileDB C API Reference
======================

.. highlight:: c

Types
-----
.. doxygentypedef:: tiledb_array_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_buffer_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_buffer_list_t
    :project: TileDB-C
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
.. doxygentypedef:: tiledb_filter_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_filter_list_t
    :project: TileDB-C
.. doxygentypedef:: tiledb_fragment_info_t
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
.. doxygendefine:: TILEDB_VAR_NUM
    :project: TileDB-C
.. doxygendefine:: TILEDB_MAX_PATH
    :project: TileDB-C
.. doxygendefine:: TILEDB_OFFSET_SIZE
    :project: TileDB-C
.. doxygendefine:: TILEDB_TIMESTAMPS
    :project: TileDB-C
.. doxygenfunction:: tiledb_var_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_max_path
    :project: TileDB-C
.. doxygenfunction:: tiledb_datatype_size
    :project: TileDB-C
.. doxygenfunction:: tiledb_offset_size
    :project: TileDB-C
.. doxygenfunction:: tiledb_timestamps
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
.. doxygenenum:: tiledb_filter_type_t
    :project: TileDB-C
.. doxygenenum:: tiledb_filter_option_t
    :project: TileDB-C
.. doxygenenum:: tiledb_walk_order_t
    :project: TileDB-C
.. doxygenenum:: tiledb_vfs_mode_t
    :project: TileDB-C
.. doxygenenum:: tiledb_mime_type_t
    :project: TileDB-C
.. doxygenenum:: tiledb_encryption_type_t
    :project: TileDB-C

Enumeration String Conversion
-----------------------------
.. doxygenfunction:: tiledb_query_type_to_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_type_from_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_object_type_to_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_object_type_from_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_filesystem_to_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_filesystem_from_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_datatype_to_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_datatype_from_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_type_to_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_type_from_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_layout_to_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_layout_from_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_filter_type_to_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_filter_type_from_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_filter_option_to_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_filter_option_from_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_encryption_type_to_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_encryption_type_from_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_status_to_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_status_from_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_walk_order_to_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_walk_order_from_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_mode_to_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_mode_from_str
    :project: TileDB-C

Context
-------
.. doxygenfunction:: tiledb_ctx_alloc
    :project: TileDB-C
.. doxygenfunction:: tiledb_ctx_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_ctx_get_stats
    :project: TileDB-C
.. doxygenfunction:: tiledb_ctx_get_config
    :project: TileDB-C
.. doxygenfunction:: tiledb_ctx_get_last_error
    :project: TileDB-C
.. doxygenfunction:: tiledb_ctx_is_supported_fs
    :project: TileDB-C
.. doxygenfunction:: tiledb_ctx_cancel_tasks
    :project: TileDB-C
.. doxygenfunction:: tiledb_ctx_set_tag
    :project: TileDB-C

Config
------
.. doxygenfunction:: tiledb_config_alloc
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
.. doxygenfunction:: tiledb_config_iter_alloc
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
.. doxygenfunction:: tiledb_array_alloc
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_set_open_timestamp_start
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_set_open_timestamp_end
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_open_timestamp_start
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_open_timestamp_end
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_delete_fragments_list
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_open
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_is_open
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_reopen
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_set_config
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_config
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_close
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_create
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_consolidate
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_vacuum
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_schema
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_query_type
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_non_empty_domain
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_non_empty_domain_from_index
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_non_empty_domain_from_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_non_empty_domain_var_size_from_index
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_non_empty_domain_var_size_from_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_non_empty_domain_var_from_index
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_non_empty_domain_var_from_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_uri
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_encryption_type
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_put_metadata
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_metadata
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_metadata_from_index
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_has_metadata_key
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_get_metadata_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_delete_metadata
    :project: TileDB-C

Array Schema
------------
.. doxygenfunction:: tiledb_array_schema_alloc
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_add_attribute
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_set_allows_dups
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_get_allows_dups
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_get_version
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_set_domain
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_set_capacity
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_set_cell_order
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_set_tile_order
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_set_coords_filter_list
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_set_offsets_filter_list
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_set_validity_filter_list
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
.. doxygenfunction:: tiledb_array_schema_get_coords_filter_list
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_get_offsets_filter_list
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_get_validity_filter_list
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
.. doxygenfunction:: tiledb_array_schema_has_attribute
    :project: TileDB-C
.. doxygenfunction:: tiledb_array_schema_dump_str
    :project: TileDB-C

Attribute
---------
.. doxygenfunction:: tiledb_attribute_alloc
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_set_nullable
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_set_filter_list
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_set_cell_val_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_get_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_get_type
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_get_nullable
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_get_filter_list
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_get_cell_val_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_get_cell_size
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_dump_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_set_fill_value
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_get_fill_value
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_set_fill_value_nullable
    :project: TileDB-C
.. doxygenfunction:: tiledb_attribute_get_fill_value_nullable
    :project: TileDB-C

Domain
------
.. doxygenfunction:: tiledb_domain_alloc
    :project: TileDB-C
.. doxygenfunction:: tiledb_domain_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_domain_get_type
    :project: TileDB-C
.. doxygenfunction:: tiledb_domain_get_ndim
    :project: TileDB-C
.. doxygenfunction:: tiledb_domain_add_dimension
    :project: TileDB-C
.. doxygenfunction:: tiledb_domain_get_dimension_from_index
    :project: TileDB-C
.. doxygenfunction:: tiledb_domain_get_dimension_from_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_domain_has_dimension
    :project: TileDB-C
.. doxygenfunction:: tiledb_domain_dump_str
    :project: TileDB-C

Dimension
---------
.. doxygenfunction:: tiledb_dimension_alloc
    :project: TileDB-C
.. doxygenfunction:: tiledb_dimension_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_dimension_get_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_dimension_get_type
    :project: TileDB-C
.. doxygenfunction:: tiledb_dimension_get_domain
    :project: TileDB-C
.. doxygenfunction:: tiledb_dimension_get_cell_val_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_dimension_set_cell_val_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_dimension_get_filter_list
    :project: TileDB-C
.. doxygenfunction:: tiledb_dimension_set_filter_list
    :project: TileDB-C
.. doxygenfunction:: tiledb_dimension_get_tile_extent
    :project: TileDB-C
.. doxygenfunction:: tiledb_dimension_dump_str
    :project: TileDB-C

Query
-----
.. doxygenfunction:: tiledb_query_alloc
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_stats
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_set_config
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_config
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_set_data_buffer
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_set_offsets_buffer
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_set_validity_buffer
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_data_buffer
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_offsets_buffer
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_validity_buffer
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_set_layout
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_set_condition
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_finalize
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_submit_and_finalize
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_submit
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_status
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_type
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_layout
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_array
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_has_results
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_est_result_size
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_est_result_size_var
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_est_result_size_nullable
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_est_result_size_var_nullable
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_fragment_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_fragment_uri
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_fragment_timestamp_range
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_set_subarray_t
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_get_subarray_t
    :project: TileDB-C

Subarray
--------
.. doxygenfunction:: tiledb_subarray_alloc
    :project: TileDB-C
.. doxygenfunction:: tiledb_subarray_set_config
    :project: TileDB-C
.. doxygenfunction:: tiledb_subarray_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_subarray_set_coalesce_ranges
    :project: TileDB-C
.. doxygenfunction:: tiledb_subarray_set_subarray
    :project: TileDB-C
.. doxygenfunction:: tiledb_subarray_add_range
    :project: TileDB-C
.. doxygenfunction:: tiledb_subarray_add_range_by_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_subarray_add_range_var
    :project: TileDB-C
.. doxygenfunction:: tiledb_subarray_add_range_var_by_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_subarray_get_range_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_subarray_get_range_num_from_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_subarray_get_range
    :project: TileDB-C
.. doxygenfunction:: tiledb_subarray_get_range_from_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_subarray_get_range_var_size
    :project: TileDB-C
.. doxygenfunction:: tiledb_subarray_get_range_var_size_from_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_subarray_get_range_var
    :project: TileDB-C
.. doxygenfunction:: tiledb_subarray_get_range_var_from_name
    :project: TileDB-C

Query Condition
---------------
.. doxygenfunction:: tiledb_query_condition_alloc
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_condition_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_condition_init
    :project: TileDB-C
.. doxygenfunction:: tiledb_query_condition_combine
    :project: TileDB-C

Filter
------
.. doxygenfunction:: tiledb_filter_alloc
    :project: TileDB-C
.. doxygenfunction:: tiledb_filter_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_filter_get_type
    :project: TileDB-C
.. doxygenfunction:: tiledb_filter_set_option
    :project: TileDB-C
.. doxygenfunction:: tiledb_filter_get_option
    :project: TileDB-C

Filter List
-----------
.. doxygenfunction:: tiledb_filter_list_alloc
    :project: TileDB-C
.. doxygenfunction:: tiledb_filter_list_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_filter_list_add_filter
    :project: TileDB-C
.. doxygenfunction:: tiledb_filter_list_set_max_chunk_size
    :project: TileDB-C
.. doxygenfunction:: tiledb_filter_list_get_nfilters
    :project: TileDB-C
.. doxygenfunction:: tiledb_filter_list_get_filter_from_index
    :project: TileDB-C
.. doxygenfunction:: tiledb_filter_list_get_max_chunk_size
    :project: TileDB-C

Group
-----
.. doxygenfunction:: tiledb_group_create
    :project: TileDB-C
.. doxygenfunction:: tiledb_group_dump_str
    :project: TileDB-C

Buffer
------
.. doxygenfunction:: tiledb_buffer_alloc
    :project: TileDB-C
.. doxygenfunction:: tiledb_buffer_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_buffer_get_type
    :project: TileDB-C
.. doxygenfunction:: tiledb_buffer_set_type
    :project: TileDB-C
.. doxygenfunction:: tiledb_buffer_get_data
    :project: TileDB-C
.. doxygenfunction:: tiledb_buffer_set_data
    :project: TileDB-C

BufferList
----------
.. doxygenfunction:: tiledb_buffer_list_alloc
    :project: TileDB-C
.. doxygenfunction:: tiledb_buffer_list_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_buffer_list_get_num_buffers
    :project: TileDB-C
.. doxygenfunction:: tiledb_buffer_list_get_total_size
    :project: TileDB-C
.. doxygenfunction:: tiledb_buffer_list_get_buffer
    :project: TileDB-C
.. doxygenfunction:: tiledb_buffer_list_flatten
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
.. doxygenfunction:: tiledb_vfs_alloc
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_get_config
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
.. doxygenfunction:: tiledb_vfs_dir_size
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_ls
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_file_size
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_move_dir
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_move_file
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_copy_dir
    :project: TileDB-C
.. doxygenfunction:: tiledb_vfs_copy_file
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

Stats
-----
.. doxygenfunction:: tiledb_stats_enable
    :project: TileDB-C
.. doxygenfunction:: tiledb_stats_disable
    :project: TileDB-C
.. doxygenfunction:: tiledb_stats_reset
    :project: TileDB-C
.. doxygenfunction:: tiledb_stats_dump
    :project: TileDB-C
.. doxygenfunction:: tiledb_stats_dump_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_stats_raw_dump
    :project: TileDB-C
.. doxygenfunction:: tiledb_stats_raw_dump_str
    :project: TileDB-C
.. doxygenfunction:: tiledb_stats_free_str
    :project: TileDB-C

Heap Profiler
-------------
.. doxygenfunction:: tiledb_heap_profiler_enable
    :project: TileDB-C

Fragment Info
-------------
.. doxygenfunction:: tiledb_fragment_info_alloc
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_set_config
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_config
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_load
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_fragment_name_v2
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_fragment_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_fragment_uri
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_fragment_size
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_dense
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_sparse
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_timestamp_range
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_non_empty_domain_from_index
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_non_empty_domain_from_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_non_empty_domain_var_size_from_index
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_non_empty_domain_var_size_from_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_non_empty_domain_var_from_index
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_non_empty_domain_var_from_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_mbr_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_mbr_from_index
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_mbr_from_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_mbr_var_size_from_index
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_mbr_var_size_from_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_mbr_var_from_index
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_mbr_var_from_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_cell_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_version
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_has_consolidated_metadata
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_unconsolidated_metadata_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_to_vacuum_num
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_to_vacuum_uri
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_array_schema
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_get_array_schema_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_dump
    :project: TileDB-C
.. doxygenfunction:: tiledb_fragment_info_dump_str
    :project: TileDB-C

Profile
-------------
.. doxygenfunction:: tiledb_profile_alloc
    :project: TileDB-C
.. doxygenfunction:: tiledb_profile_free
    :project: TileDB-C
.. doxygenfunction:: tiledb_profile_get_name
    :project: TileDB-C
.. doxygenfunction:: tiledb_profile_get_homedir
    :project: TileDB-C
.. doxygenfunction:: tiledb_profile_set_param
    :project: TileDB-C
.. doxygenfunction:: tiledb_profile_get_param
    :project: TileDB-C

Experimental
-------------
.. autodoxygenfile:: tiledb_experimental.h
    :project: TileDB-C

Deprecated
-------------
.. autodoxygenfile:: tiledb_deprecated.h
    :project: TileDB-C

Serialization
-------------
.. autodoxygenfile:: tiledb_serialization.h
    :project: TileDB-C
