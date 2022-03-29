tags 0.1.0..2.0.0
commit c87fd225965257fb5b069e4abf8e0ebed5f84fc4
Date:   Tue Apr 28 16:35:59 2020 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_max_buffer_size(
     tiledb_ctx_t* ctx,
     tiledb_array_t* array,
     const char* name,
commit c87fd225965257fb5b069e4abf8e0ebed5f84fc4
Date:   Tue Apr 28 16:35:59 2020 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_max_buffer_size_var(
     tiledb_ctx_t* ctx,
     tiledb_array_t* array,
     const char* name,
commit e0bee941c0295346755daaf2b37c95c950d6a7dd
Date:   Tue Apr 28 15:18:45 2020 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT const char* tiledb_coords();
 
 /** Returns a special value indicating a variable number of elements. */
 TILEDB_EXPORT uint32_t tiledb_var_num();
commit 34fc0f3d8fc50d015eea9a3636263df6eddd1d24
Date:   Mon Apr 13 17:10:58 2020 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_consolidate_metadata(
     tiledb_ctx_t* ctx, const char* array_uri, tiledb_config_t* config);
 
 /**
commit 34fc0f3d8fc50d015eea9a3636263df6eddd1d24
Date:   Mon Apr 13 17:10:58 2020 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_consolidate_metadata_with_key(
     tiledb_ctx_t* ctx,
     const char* array_uri,
     tiledb_encryption_type_t encryption_type,
commit 64334fe41278d1195c9e76a6f067bb8e7bdac342
Date:   Tue Jul 9 09:50:34 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
@@ -4567,7 +4567,7 @@ TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_create_with_key(
  * @param ctx The TileDB context.
  * @param kv_uri The name of the TileDB key-value store to be consolidated.
  * @param config Configuration parameters for the consolidation
commit 64334fe41278d1195c9e76a6f067bb8e7bdac342
Date:   Tue Jul 9 09:50:34 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
@@ -4594,7 +4594,7 @@ TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_consolidate(
  * @param encryption_key The encryption key to use.
  * @param key_length Length in bytes of the encryption key.
  * @param config Configuration parameters for the consolidation
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
 tiledb_kv_schema_alloc(tiledb_ctx_t* ctx, tiledb_kv_schema_t** kv_schema);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED void tiledb_kv_schema_free(
+    tiledb_kv_schema_t** kv_schema);
 
 /**
  * Adds an attribute to a key-value schema.
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_schema_add_attribute(
     tiledb_ctx_t* ctx, tiledb_kv_schema_t* kv_schema, tiledb_attribute_t* attr);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_schema_set_capacity(
     tiledb_ctx_t* ctx, tiledb_kv_schema_t* kv_schema, uint64_t capacity);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
 tiledb_kv_schema_check(tiledb_ctx_t* ctx, tiledb_kv_schema_t* kv_schema);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_schema_load(
     tiledb_ctx_t* ctx, const char* kv_uri, tiledb_kv_schema_t** kv_schema);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_schema_load_with_key(
     tiledb_ctx_t* ctx,
     const char* kv_uri,
     tiledb_encryption_type_t encryption_type,
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_schema_get_capacity(
     tiledb_ctx_t* ctx, const tiledb_kv_schema_t* kv_schema, uint64_t* capacity);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_schema_get_attribute_num(
     tiledb_ctx_t* ctx,
     const tiledb_kv_schema_t* kv_schema,
     uint32_t* attribute_num);
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
+tiledb_kv_schema_get_attribute_from_index(
     tiledb_ctx_t* ctx,
     const tiledb_kv_schema_t* kv_schema,
     uint32_t index,
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
+tiledb_kv_schema_get_attribute_from_name(
     tiledb_ctx_t* ctx,
     const tiledb_kv_schema_t* kv_schema,
     const char* name,
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_schema_has_attribute(
     tiledb_ctx_t* ctx,
     const tiledb_kv_schema_t* kv_schema,
     const char* name,
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_schema_dump(
     tiledb_ctx_t* ctx, const tiledb_kv_schema_t* kv_schema, FILE* out);
 
 /* ****************************** */
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
 tiledb_kv_item_alloc(tiledb_ctx_t* ctx, tiledb_kv_item_t** kv_item);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED void tiledb_kv_item_free(
+    tiledb_kv_item_t** kv_item);
 
 /**
  * Set the key in the key-value item.
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_item_set_key(
     tiledb_ctx_t* ctx,
     tiledb_kv_item_t* kv_item,
     const void* key,
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_item_set_value(
     tiledb_ctx_t* ctx,
     tiledb_kv_item_t* kv_item,
     const char* attribute,
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_item_get_key(
     tiledb_ctx_t* ctx,
     tiledb_kv_item_t* kv_item,
     const void** key,
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_item_get_value(
     tiledb_ctx_t* ctx,
     tiledb_kv_item_t* kv_item,
     const char* attribute,
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_create(
     tiledb_ctx_t* ctx, const char* kv_uri, const tiledb_kv_schema_t* kv_schema);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_create_with_key(
     tiledb_ctx_t* ctx,
     const char* kv_uri,
     const tiledb_kv_schema_t* kv_schema,
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_consolidate(
     tiledb_ctx_t* ctx, const char* kv_uri, tiledb_config_t* config);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_consolidate_with_key(
     tiledb_ctx_t* ctx,
     const char* kv_uri,
     tiledb_encryption_type_t encryption_type,
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
 tiledb_kv_alloc(tiledb_ctx_t* ctx, const char* kv_uri, tiledb_kv_t** kv);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_open(
     tiledb_ctx_t* ctx, tiledb_kv_t* kv, tiledb_query_type_t query_type);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_open_at(
     tiledb_ctx_t* ctx,
     tiledb_kv_t* kv,
     tiledb_query_type_t query_type,
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_open_with_key(
     tiledb_ctx_t* ctx,
     tiledb_kv_t* kv,
     tiledb_query_type_t query_type,
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_open_at_with_key(
     tiledb_ctx_t* ctx,
     tiledb_kv_t* kv,
     tiledb_query_type_t query_type,
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
 tiledb_kv_is_open(tiledb_ctx_t* ctx, tiledb_kv_t* kv, int32_t* is_open);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
+tiledb_kv_reopen(tiledb_ctx_t* ctx, tiledb_kv_t* kv);
 
 /**
  * Reopens a key-value store at a specific timestamp.
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
 tiledb_kv_reopen_at(tiledb_ctx_t* ctx, tiledb_kv_t* kv, uint64_t timestamp);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_get_timestamp(
     tiledb_ctx_t* ctx, tiledb_kv_t* kv, uint64_t* timestamp);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
+tiledb_kv_close(tiledb_ctx_t* ctx, tiledb_kv_t* kv);
 
 /**
  * Frees the key-value store object.
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED void tiledb_kv_free(tiledb_kv_t** kv);
 
 /**
  * Retrieves the schema of a kv object.
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_get_schema(
     tiledb_ctx_t* ctx, tiledb_kv_t* kv, tiledb_kv_schema_t** kv_schema);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
 tiledb_kv_is_dirty(tiledb_ctx_t* ctx, tiledb_kv_t* kv, int32_t* is_dirty);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_add_item(
     tiledb_ctx_t* ctx, tiledb_kv_t* kv, tiledb_kv_item_t* kv_item);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
+tiledb_kv_flush(tiledb_ctx_t* ctx, tiledb_kv_t* kv);
 
 /**
  * Retrieves a key-value item based on the input key. If the item with
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_get_item(
     tiledb_ctx_t* ctx,
     tiledb_kv_t* kv,
     const void* key,
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_has_key(
     tiledb_ctx_t* ctx,
     tiledb_kv_t* kv,
     const void* key,
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_encryption_type(
     tiledb_ctx_t* ctx,
     const char* kv_uri,
     tiledb_encryption_type_t* encryption_type);
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_iter_alloc(
     tiledb_ctx_t* ctx, tiledb_kv_t* kv, tiledb_kv_iter_t** kv_iter);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED void tiledb_kv_iter_free(
+    tiledb_kv_iter_t** kv_iter);
 
 /**
  * Retrieves the key-value item currently pointed by the iterator.
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_iter_here(
     tiledb_ctx_t* ctx, tiledb_kv_iter_t* kv_iter, tiledb_kv_item_t** kv_item);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
 tiledb_kv_iter_next(tiledb_ctx_t* ctx, tiledb_kv_iter_t* kv_iter);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_kv_iter_done(
     tiledb_ctx_t* ctx, tiledb_kv_iter_t* kv_iter, int32_t* done);
 
 /**
commit fac8e765a33398917f1f33801aa5d13918955075
Date:   Tue Jun 25 09:13:32 2019 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
 tiledb_kv_iter_reset(tiledb_ctx_t* ctx, tiledb_kv_iter_t* kv_iter);
 
 /* ****************************** */
commit 1c6a6c98846635a4b3c5ee4160b965fc4506853e
Date:   Wed Oct 3 13:39:17 2018 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_attribute_set_compressor(
     tiledb_ctx_t* ctx,
     tiledb_attribute_t* attr,
     tiledb_compressor_t compressor,
commit 1c6a6c98846635a4b3c5ee4160b965fc4506853e
Date:   Wed Oct 3 13:39:17 2018 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t tiledb_attribute_get_compressor(
     tiledb_ctx_t* ctx,
     const tiledb_attribute_t* attr,
     tiledb_compressor_t* compressor,
commit 1c6a6c98846635a4b3c5ee4160b965fc4506853e
Date:   Wed Oct 3 13:39:17 2018 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
+tiledb_array_schema_set_coords_compressor(
     tiledb_ctx_t* ctx,
     tiledb_array_schema_t* array_schema,
     tiledb_compressor_t compressor,
commit 1c6a6c98846635a4b3c5ee4160b965fc4506853e
Date:   Wed Oct 3 13:39:17 2018 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
+tiledb_array_schema_set_offsets_compressor(
     tiledb_ctx_t* ctx,
     tiledb_array_schema_t* array_schema,
     tiledb_compressor_t compressor,
commit 1c6a6c98846635a4b3c5ee4160b965fc4506853e
Date:   Wed Oct 3 13:39:17 2018 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
+tiledb_array_schema_get_coords_compressor(
     tiledb_ctx_t* ctx,
     const tiledb_array_schema_t* array_schema,
     tiledb_compressor_t* compressor,
commit 1c6a6c98846635a4b3c5ee4160b965fc4506853e
Date:   Wed Oct 3 13:39:17 2018 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_EXPORT TILEDB_DEPRECATED int32_t
+tiledb_array_schema_get_offsets_compressor(
     tiledb_ctx_t* ctx,
     const tiledb_array_schema_t* array_schema,
     tiledb_compressor_t* compressor,
commit 9e31b22e9fe6010459241c26108f70d5f3f963e3
Date:   Mon Jan 8 15:10:48 2018 -0500
diff --git a/core/include/c_api/tiledb.h b/core/include/c_api/tiledb.h
+#define TILEDB_DEPRECATED __declspec(deprecated)
 #else
 #define DEPRECATED
 #pragma message("TILEDB_DEPRECATED is not defined for this compiler")
commit 341e7638ab3f849ceb64c9185daadddd6b240791
Date:   Sun Dec 10 17:27:42 2017 -0500
diff --git a/core/include/c_api/tiledb.h b/core/include/c_api/tiledb.h
@@ -1344,6 +1389,221 @@ TILEDB_DEPRECATED int tiledb_attribute_iter_here(
 TILEDB_DEPRECATED int tiledb_attribute_iter_first(
     tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it);
 
+/* ****************************** */
commit 1cd7d837020cc12dffe366f5ad9ccd5686e0328a
Date:   Thu Dec 14 11:06:31 2017 -0700
diff --git a/core/include/c_api/tiledb.h b/core/include/c_api/tiledb.h
+#define TILEDB_DEPRECATED __attribute__((deprecated, visibility("default")))
+#else
+#define DEPRECATED
+#pragma message("TILEDB_DEPRECATED is not defined for this compiler")
+#endif
commit 1cd7d837020cc12dffe366f5ad9ccd5686e0328a
Date:   Thu Dec 14 11:06:31 2017 -0700
diff --git a/core/include/c_api/tiledb.h b/core/include/c_api/tiledb.h
+TILEDB_DEPRECATED int tiledb_dimension_iter_create(
+    tiledb_ctx_t* ctx,
+    const tiledb_domain_t* domain,
+    tiledb_dimension_iter_t** dim_it);
+
commit 1cd7d837020cc12dffe366f5ad9ccd5686e0328a
Date:   Thu Dec 14 11:06:31 2017 -0700
diff --git a/core/include/c_api/tiledb.h b/core/include/c_api/tiledb.h
+TILEDB_DEPRECATED int tiledb_dimension_iter_free(
+    tiledb_ctx_t* ctx, tiledb_dimension_iter_t* dim_it);
+
+/**
+ * Checks if a dimension iterator has reached the end.
commit 1cd7d837020cc12dffe366f5ad9ccd5686e0328a
Date:   Thu Dec 14 11:06:31 2017 -0700
diff --git a/core/include/c_api/tiledb.h b/core/include/c_api/tiledb.h
+TILEDB_DEPRECATED int tiledb_dimension_iter_done(
+    tiledb_ctx_t* ctx, tiledb_dimension_iter_t* dim_it, int* done);
+
+/**
+ * Advances the dimension iterator.
commit 1cd7d837020cc12dffe366f5ad9ccd5686e0328a
Date:   Thu Dec 14 11:06:31 2017 -0700
diff --git a/core/include/c_api/tiledb.h b/core/include/c_api/tiledb.h
+TILEDB_DEPRECATED int tiledb_dimension_iter_next(
+    tiledb_ctx_t* ctx, tiledb_dimension_iter_t* dim_it);
+
+/**
+ * Retrieves a constant pointer to the current dimension pointed by the
commit 1cd7d837020cc12dffe366f5ad9ccd5686e0328a
Date:   Thu Dec 14 11:06:31 2017 -0700
diff --git a/core/include/c_api/tiledb.h b/core/include/c_api/tiledb.h
+TILEDB_DEPRECATED int tiledb_dimension_iter_here(
+    tiledb_ctx_t* ctx,
+    tiledb_dimension_iter_t* dim_it,
+    const tiledb_dimension_t** dim);
+
commit 1cd7d837020cc12dffe366f5ad9ccd5686e0328a
Date:   Thu Dec 14 11:06:31 2017 -0700
diff --git a/core/include/c_api/tiledb.h b/core/include/c_api/tiledb.h
+TILEDB_DEPRECATED int tiledb_dimension_iter_first(
+    tiledb_ctx_t* ctx, tiledb_dimension_iter_t* dim_it);
+
+/**
+ * Creates an attribute iterator for the input array metadata.
commit 1cd7d837020cc12dffe366f5ad9ccd5686e0328a
Date:   Thu Dec 14 11:06:31 2017 -0700
diff --git a/core/include/c_api/tiledb.h b/core/include/c_api/tiledb.h
+TILEDB_DEPRECATED int tiledb_attribute_iter_create(
+    tiledb_ctx_t* ctx,
+    const tiledb_array_metadata_t* metadata,
+    tiledb_attribute_iter_t** attr_it);
+
commit 1cd7d837020cc12dffe366f5ad9ccd5686e0328a
Date:   Thu Dec 14 11:06:31 2017 -0700
diff --git a/core/include/c_api/tiledb.h b/core/include/c_api/tiledb.h
+TILEDB_DEPRECATED int tiledb_attribute_iter_free(
+    tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it);
+
+/**
+ * Checks if an attribute iterator has reached the end.
commit 1cd7d837020cc12dffe366f5ad9ccd5686e0328a
Date:   Thu Dec 14 11:06:31 2017 -0700
diff --git a/core/include/c_api/tiledb.h b/core/include/c_api/tiledb.h
+TILEDB_DEPRECATED int tiledb_attribute_iter_done(
+    tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it, int* done);
+
+/**
+ * Advances the attribute iterator.
commit 1cd7d837020cc12dffe366f5ad9ccd5686e0328a
Date:   Thu Dec 14 11:06:31 2017 -0700
diff --git a/core/include/c_api/tiledb.h b/core/include/c_api/tiledb.h
+TILEDB_DEPRECATED int tiledb_attribute_iter_next(
+    tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it);
+
+/**
+ * Retrieves a constant pointer to the current attribute pointed by the
commit 1cd7d837020cc12dffe366f5ad9ccd5686e0328a
Date:   Thu Dec 14 11:06:31 2017 -0700
diff --git a/core/include/c_api/tiledb.h b/core/include/c_api/tiledb.h
+TILEDB_DEPRECATED int tiledb_attribute_iter_here(
+    tiledb_ctx_t* ctx,
+    tiledb_attribute_iter_t* attr_it,
+    const tiledb_attribute_t** attr);
+
commit 1cd7d837020cc12dffe366f5ad9ccd5686e0328a
Date:   Thu Dec 14 11:06:31 2017 -0700
diff --git a/core/include/c_api/tiledb.h b/core/include/c_api/tiledb.h
+TILEDB_DEPRECATED int tiledb_attribute_iter_first(
+    tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it);
+

tags 2.0.0..2.1.0
-nothing-

tags 2.1.0..2.2.0-rc1
-nothing-

tags 2.2.0-rc1..2.3.0
commit 2241c42ed832b896232488828802b6c70e6f6334
Date:   Wed Mar 24 14:09:45 2021 +0100
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT const char* tiledb_coords(void);

tags 2.3.0..2.4.0
commit 8c1d25f018e35f9a565844e3cd0aaca4b2c87104
Date:   Mon Aug 23 16:12:21 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
@@ -6234,26 +6152,6 @@ TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_consolidate_metadata_with_key(
     uint32_t key_length,
     tiledb_config_t* config);
 
commit 4433bf466870b3345b0a66a71e3f007907cdf526
Date:   Sat Aug 21 14:18:31 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
@@ -6120,6 +6202,26 @@ TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_consolidate_metadata_with_key(
     uint32_t key_length,
     tiledb_config_t* config);
 
+/**
commit ad4e0b6b9cbbf93aa7f01b70730926b6ef79aa82
Date:   Fri Aug 6 07:30:55 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_schema_load_with_key(
     tiledb_ctx_t* ctx,
     const char* array_uri,
     tiledb_encryption_type_t encryption_type,
commit ad4e0b6b9cbbf93aa7f01b70730926b6ef79aa82
Date:   Fri Aug 6 07:30:55 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_open_at(
     tiledb_ctx_t* ctx,
     tiledb_array_t* array,
     tiledb_query_type_t query_type,
     uint64_t timestamp);
commit ad4e0b6b9cbbf93aa7f01b70730926b6ef79aa82
Date:   Fri Aug 6 07:30:55 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_open_with_key(
     tiledb_ctx_t* ctx,
     tiledb_array_t* array,
     tiledb_query_type_t query_type,
commit ad4e0b6b9cbbf93aa7f01b70730926b6ef79aa82
Date:   Fri Aug 6 07:30:55 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_open_at_with_key(
     tiledb_ctx_t* ctx,
     tiledb_array_t* array,
     tiledb_query_type_t query_type,
commit ad4e0b6b9cbbf93aa7f01b70730926b6ef79aa82
Date:   Fri Aug 6 07:30:55 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_reopen_at(
     tiledb_ctx_t* ctx, tiledb_array_t* array, uint64_t timestamp);
 
 /**
commit ad4e0b6b9cbbf93aa7f01b70730926b6ef79aa82
Date:   Fri Aug 6 07:30:55 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_get_timestamp(
     tiledb_ctx_t* ctx, tiledb_array_t* array, uint64_t* timestamp);
 
 /**
commit ad4e0b6b9cbbf93aa7f01b70730926b6ef79aa82
Date:   Fri Aug 6 07:30:55 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_create_with_key(
     tiledb_ctx_t* ctx,
     const char* array_uri,
     const tiledb_array_schema_t* array_schema,
commit ad4e0b6b9cbbf93aa7f01b70730926b6ef79aa82
Date:   Fri Aug 6 07:30:55 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_consolidate_with_key(
     tiledb_ctx_t* ctx,
     const char* array_uri,
     tiledb_encryption_type_t encryption_type,
commit ad4e0b6b9cbbf93aa7f01b70730926b6ef79aa82
Date:   Fri Aug 6 07:30:55 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
@@ -6103,8 +6088,6 @@ TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_consolidate_metadata(
     tiledb_ctx_t* ctx, const char* array_uri, tiledb_config_t* config);
 
 /**
commit ad4e0b6b9cbbf93aa7f01b70730926b6ef79aa82
Date:   Fri Aug 6 07:30:55 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_fragment_info_load_with_key(
     tiledb_ctx_t* ctx,
     tiledb_fragment_info_t* fragment_info,
     tiledb_encryption_type_t encryption_type,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_set_buffer(
     tiledb_ctx_t* ctx,
     tiledb_query_t* query,
     const char* name,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_set_buffer_var(
     tiledb_ctx_t* ctx,
     tiledb_query_t* query,
     const char* name,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_set_buffer_nullable(
     tiledb_ctx_t* ctx,
     tiledb_query_t* query,
     const char* name,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_set_buffer_var_nullable(
     tiledb_ctx_t* ctx,
     tiledb_query_t* query,
     const char* name,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_buffer(
     tiledb_ctx_t* ctx,
     tiledb_query_t* query,
     const char* name,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_buffer_var(
     tiledb_ctx_t* ctx,
     tiledb_query_t* query,
     const char* name,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_buffer_nullable(
     tiledb_ctx_t* ctx,
     tiledb_query_t* query,
     const char* name,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_buffer_var_nullable(
     tiledb_ctx_t* ctx,
     tiledb_query_t* query,
     const char* name,
commit 5658e01d24a70cca9bd60872980ec2703693e945
Date:   Fri May 21 08:37:36 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
@@ -5743,6 +5751,8 @@ TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_consolidate_metadata(
     tiledb_ctx_t* ctx, const char* array_uri, tiledb_config_t* config);
 

tags 2.4.0..2.5.0
commit 18846b62b10ac654453b9f3f3ce54900e410877c
Date:   Mon Aug 23 11:29:37 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
@@ -6234,26 +6152,6 @@ TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_consolidate_metadata_with_key(
     uint32_t key_length,
     tiledb_config_t* config);
 
commit a7f90975ad0dfd2e94a49f97a81010ef41c0349f
Date:   Sat Aug 21 08:37:41 2021 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
@@ -6120,6 +6202,26 @@ TILEDB_DEPRECATED_EXPORT int32_t tiledb_array_consolidate_metadata_with_key(
     uint32_t key_length,
     tiledb_config_t* config);
 
+/**

tags 2.5.0..2.6.0
commit 7ef95b9323dd6e5e86dfed6b1ea99e312e7d5db3
Date:   Thu Jan 6 07:27:35 2022 -0500
diff --git a/tiledb/sm/c_api/tiledb_experimental.h b/tiledb/sm/c_api/tiledb_experimental.h
@@ -206,6 +206,51 @@ TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_add_point_ranges(
     const void* start,
     uint64_t count);
 
commit 72b7dc83fd10a55d11faf2623f878f55a6ee0448
Date:   Tue Dec 21 11:07:05 2021 -0500
diff --git a/tiledb/sm/c_api/tiledb_experimental.h b/tiledb/sm/c_api/tiledb_experimental.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_add_point_ranges(
+    tiledb_ctx_t* ctx,
+    tiledb_query_t* query,
+    uint32_t dim_idx,
+    const void* start,

tags 2.6.0..2.7.0
commit fd773ef4ba590b026040472660984ea29a8afb60
Date:   Thu Jan 13 10:05:24 2022 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+#define TILEDB_STRING_UCS2 TILEDB_DEPRECATED TILEDB_STRING_UCS2_VAL
+#undef TILEDB_STRING_UCS2_VAL
+#endif
+#ifdef TILEDB_STRING_UCS4
+#def TILEDB_STRING_UCS4_VAL TILEDB_STRING_UCS4
commit fd773ef4ba590b026040472660984ea29a8afb60
Date:   Thu Jan 13 10:05:24 2022 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+#define TILEDB_STRING_UCS4 TILEDB_DEPRECATED TILEDB_STRING_UCS4_VAL
+#undef TILEDB_STRING_UCS4_VAL
+#endif
 #ifdef TILEDB_ANY
 #def TILEDB_ANY_VAL TILEDB_ANY
commit 2bf273c37883065d54cb3aeac9136e51d223719f
Date:   Wed Jan 12 07:32:26 2022 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+#define TILEDB_ANY TILEDB_DEPRECATED TILEDB_ANY_VAL
+#undef TILEDB_ANY_VAL
+#endif
 } tiledb_datatype_t;
 
commit 81ed6a8ebcce26e13745fe335166c8cdc3282787
Date:   Fri Jan 7 14:16:23 2022 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+#define TILEDB_CHAR TILEDB_DEPRECATED TILEDB_CHAR_VAL
+#undef TILEDB_CHAR_VAL
+#endif
 } tiledb_datatype_t;
 
commit c66941deb34304926e103ede1078a3b6d836dee3
Date:   Wed Jan 5 18:07:08 2022 -0500
diff --git a/tiledb/sm/c_api/tiledb_experimental.h b/tiledb/sm/c_api/tiledb_experimental.h
@@ -218,6 +218,51 @@ TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_add_point_ranges(
     const void* start,
     uint64_t count);
 
commit 96f2f3ba3f9af526829ad0c60fd3d34c7d6a5007
Date:   Tue Dec 21 11:07:05 2021 -0500
diff --git a/tiledb/sm/c_api/tiledb_experimental.h b/tiledb/sm/c_api/tiledb_experimental.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_add_point_ranges(
+    tiledb_ctx_t* ctx,
+    tiledb_query_t* query,
+    uint32_t dim_idx,
+    const void* start,
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+#ifndef TILEDB_DEPRECATED
+#define TILEDB_DEPRECATED
+#endif
+#ifndef TILEDB_DEPRECATED_EXPORT
+#define TILEDB_DEPRECATED_EXPORT
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_set_subarray(
+    tiledb_ctx_t* ctx, tiledb_query_t* query, const void* subarray);
+
+/**
+ * Indicates that the query will write or read a subarray, and provides
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_add_range(
     tiledb_ctx_t* ctx,
     tiledb_query_t* query,
     uint32_t dim_idx,
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_add_range_by_name(
     tiledb_ctx_t* ctx,
     tiledb_query_t* query,
     const char* dim_name,
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_add_range_var(
     tiledb_ctx_t* ctx,
     tiledb_query_t* query,
     uint32_t dim_idx,
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_add_range_var_by_name(
     tiledb_ctx_t* ctx,
     tiledb_query_t* query,
     const char* dim_name,
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_range_num(
     tiledb_ctx_t* ctx,
     const tiledb_query_t* query,
     uint32_t dim_idx,
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_range_num_from_name(
     tiledb_ctx_t* ctx,
     const tiledb_query_t* query,
     const char* dim_name,
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_range(
     tiledb_ctx_t* ctx,
     const tiledb_query_t* query,
     uint32_t dim_idx,
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_range_from_name(
     tiledb_ctx_t* ctx,
     const tiledb_query_t* query,
     const char* dim_name,
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_range_var_size(
     tiledb_ctx_t* ctx,
     const tiledb_query_t* query,
     uint32_t dim_idx,
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_range_var_size_from_name(
     tiledb_ctx_t* ctx,
     const tiledb_query_t* query,
     const char* dim_name,
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_range_var(
     tiledb_ctx_t* ctx,
     const tiledb_query_t* query,
     uint32_t dim_idx,
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_get_range_var_from_name(
     tiledb_ctx_t* ctx,
     const tiledb_query_t* query,
     const char* dim_name,

tags 2.7.0..2.8.0-rc0
commit 6dd6d0fda7bbf831f7208d9e9f2ecb03418c52cb
Date:   Fri Mar 25 15:42:42 2022 -0400
diff --git a/tiledb/sm/c_api/tiledb.h b/tiledb/sm/c_api/tiledb.h
+TILEDB_DEPRECATED_EXPORT int32_t
 tiledb_group_create(tiledb_ctx_t* ctx, const char* group_uri);

