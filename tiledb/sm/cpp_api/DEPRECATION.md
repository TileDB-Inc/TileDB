tags 0.1.0..2.0.0
commit c87fd225965257fb5b069e4abf8e0ebed5f84fc4
Date:   Tue Apr 28 16:35:59 2020 -0400
diff --git a/tiledb/sm/cpp_api/array.h b/tiledb/sm/cpp_api/array.h
+  TILEDB_DEPRECATED
+      std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
+      max_buffer_elements(const std::vector<T>& subarray) {
     auto ctx = ctx_.get();
     tiledb_ctx_t* c_ctx = ctx.ptr().get();

commit e0bee941c0295346755daaf2b37c95c950d6a7dd
Date:   Tue Apr 28 15:18:45 2020 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_coordinates(T* buf, uint64_t size) {
     impl::type_check<T>(schema_.domain().type());
     return set_buffer(TILEDB_COORDS, buf, size);
   }
 
commit e0bee941c0295346755daaf2b37c95c950d6a7dd
Date:   Tue Apr 28 15:18:45 2020 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_coordinates(Vec& buf) {
     return set_coordinates(buf.data(), buf.size());
   }
 
commit 1c6a6c98846635a4b3c5ee4160b965fc4506853e
Date:   Wed Oct 3 13:39:17 2018 -0400
diff --git a/tiledb/sm/cpp_api/array_schema.h b/tiledb/sm/cpp_api/array_schema.h
+  TILEDB_DEPRECATED Compressor coords_compressor() const {
+    return get_compressor(coords_filter_list());
   }
 
   /**
commit 1c6a6c98846635a4b3c5ee4160b965fc4506853e
Date:   Wed Oct 3 13:39:17 2018 -0400
diff --git a/tiledb/sm/cpp_api/array_schema.h b/tiledb/sm/cpp_api/array_schema.h
+  TILEDB_DEPRECATED ArraySchema& set_coords_compressor(const Compressor& c) {
+    if (coords_filter_list().nfilters() > 0)
+      throw TileDBError(
+          "[TileDB::C++API] Error: Cannot add second filter with "
+          "deprecated API.");
commit 1c6a6c98846635a4b3c5ee4160b965fc4506853e
Date:   Wed Oct 3 13:39:17 2018 -0400
diff --git a/tiledb/sm/cpp_api/array_schema.h b/tiledb/sm/cpp_api/array_schema.h
+  TILEDB_DEPRECATED Compressor offsets_compressor() const {
+    return get_compressor(offsets_filter_list());
   }
 
   /**
commit 1c6a6c98846635a4b3c5ee4160b965fc4506853e
Date:   Wed Oct 3 13:39:17 2018 -0400
diff --git a/tiledb/sm/cpp_api/array_schema.h b/tiledb/sm/cpp_api/array_schema.h
+  TILEDB_DEPRECATED ArraySchema& set_offsets_compressor(const Compressor& c) {
+    if (offsets_filter_list().nfilters() > 0)
+      throw TileDBError(
+          "[TileDB::C++API] Error: Cannot add second filter with "
+          "deprecated API.");
commit 1c6a6c98846635a4b3c5ee4160b965fc4506853e
Date:   Wed Oct 3 13:39:17 2018 -0400
diff --git a/tiledb/sm/cpp_api/attribute.h b/tiledb/sm/cpp_api/attribute.h
+  TILEDB_DEPRECATED Attribute(
       const Context& ctx,
       const std::string& name,
       tiledb_datatype_t type,
       const Compressor& compressor)
commit 1c6a6c98846635a4b3c5ee4160b965fc4506853e
Date:   Wed Oct 3 13:39:17 2018 -0400
diff --git a/tiledb/sm/cpp_api/attribute.h b/tiledb/sm/cpp_api/attribute.h
+  TILEDB_DEPRECATED Compressor compressor() const {
+    FilterList filters = filter_list();
+    for (uint32_t i = 0; i < filters.nfilters(); i++) {
+      auto f = filters.filter(i);
+      int32_t level;
commit 1c6a6c98846635a4b3c5ee4160b965fc4506853e
Date:   Wed Oct 3 13:39:17 2018 -0400
diff --git a/tiledb/sm/cpp_api/attribute.h b/tiledb/sm/cpp_api/attribute.h
+  TILEDB_DEPRECATED Attribute& set_compressor(Compressor c) {
+    if (filter_list().nfilters() > 0)
+      throw TileDBError(
+          "[TileDB::C++API] Error: Cannot add second filter with "
+          "deprecated API.");
commit 1c6a6c98846635a4b3c5ee4160b965fc4506853e
Date:   Wed Oct 3 13:39:17 2018 -0400
diff --git a/tiledb/sm/cpp_api/attribute.h b/tiledb/sm/cpp_api/attribute.h
+  TILEDB_DEPRECATED static Attribute create(
       const Context& ctx,
       const std::string& name,
       const Compressor& compressor) {
+    FilterList filter_list(ctx);


tags 2.0.0..2.1.0
-nothing-

tags 2.1.0..2.2.0-rc1
-nothing-

tags tags 2.2.0-rc1..2.3.0
commit 4d2d8aed2f84aee6a513809f87e3e3e699575d9c
Date:   Thu May 20 10:33:20 2021 -0400
diff --git a/tiledb/sm/cpp_api/array.h b/tiledb/sm/cpp_api/array.h
+  TILEDB_DEPRECATED
+  uint64_t timestamp() const {
     auto& ctx = ctx_.get();
     uint64_t timestamp_end;
     ctx.handle_error(tiledb_array_get_timestamp(
commit 49af7438d71f851e514843e1a25b57f0041c2d41
Date:   Tue Apr 20 11:51:53 2021 -0400
diff --git a/tiledb/sm/cpp_api/array.h b/tiledb/sm/cpp_api/array.h
+  TILEDB_DEPRECATED
   Array(
       const Context& ctx,
       const std::string& array_uri,
       tiledb_query_type_t query_type,
commit 49af7438d71f851e514843e1a25b57f0041c2d41
Date:   Tue Apr 20 11:51:53 2021 -0400
diff --git a/tiledb/sm/cpp_api/array.h b/tiledb/sm/cpp_api/array.h
+  TILEDB_DEPRECATED
   Array(
       const Context& ctx,
       const std::string& array_uri,
commit 49af7438d71f851e514843e1a25b57f0041c2d41
Date:   Tue Apr 20 11:51:53 2021 -0400
diff --git a/tiledb/sm/cpp_api/array.h b/tiledb/sm/cpp_api/array.h
+  TILEDB_DEPRECATED
   Array(
       const Context& ctx,
       const std::string& array_uri,
       tiledb_query_type_t query_type,
commit 49af7438d71f851e514843e1a25b57f0041c2d41
Date:   Tue Apr 20 11:51:53 2021 -0400
diff --git a/tiledb/sm/cpp_api/array.h b/tiledb/sm/cpp_api/array.h
+  TILEDB_DEPRECATED
+  void open(tiledb_query_type_t query_type, uint64_t timestamp_end) {
+    open(query_type, TILEDB_NO_ENCRYPTION, nullptr, 0, timestamp_end);
   }
 
commit 49af7438d71f851e514843e1a25b57f0041c2d41
Date:   Tue Apr 20 11:51:53 2021 -0400
diff --git a/tiledb/sm/cpp_api/array.h b/tiledb/sm/cpp_api/array.h
+  TILEDB_DEPRECATED
   void open(
       tiledb_query_type_t query_type,
       tiledb_encryption_type_t encryption_type,
       const void* encryption_key,
commit 49af7438d71f851e514843e1a25b57f0041c2d41
Date:   Tue Apr 20 11:51:53 2021 -0400
diff --git a/tiledb/sm/cpp_api/array.h b/tiledb/sm/cpp_api/array.h
+  TILEDB_DEPRECATED
+  void reopen_at(uint64_t timestamp_end) {
     auto& ctx = ctx_.get();
     tiledb_ctx_t* c_ctx = ctx.ptr().get();

tags 2.3.0..2.4.0
commit 5658e01d24a70cca9bd60872980ec2703693e945
Date:   Fri May 21 08:37:36 2021 -0400
diff --git a/tiledb/sm/cpp_api/array.h b/tiledb/sm/cpp_api/array.h
+  TILEDB_DEPRECATED
   Array(
       const Context& ctx,
       const std::string& array_uri,
commit 5658e01d24a70cca9bd60872980ec2703693e945
Date:   Fri May 21 08:37:36 2021 -0400
diff --git a/tiledb/sm/cpp_api/array.h b/tiledb/sm/cpp_api/array.h
+  TILEDB_DEPRECATED
   void open(
       tiledb_query_type_t query_type,
       tiledb_encryption_type_t encryption_type,
commit a0c8b15fca009a0d852f6a180e0f10e6c48f5648
Date:   Wed May 19 19:04:22 2021 -0400
diff --git a/tiledb/sm/cpp_api/array.h b/tiledb/sm/cpp_api/array.h
+  TILEDB_DEPRECATED
+  uint64_t timestamp() const {
     auto& ctx = ctx_.get();
     uint64_t timestamp_end;
     ctx.handle_error(tiledb_array_get_timestamp(
commit 3b5128a2df7b5429da9f484622911dceb158b89b
Date:   Mon Jun 7 11:56:47 2021 -0400
diff --git a/tiledb/sm/cpp_api/array_schema.h b/tiledb/sm/cpp_api/array_schema.h
+  TILEDB_DEPRECATED
   ArraySchema(
       const Context& ctx,
       const std::string& uri,
commit 3b5128a2df7b5429da9f484622911dceb158b89b
Date:   Mon Jun 7 11:56:47 2021 -0400
diff --git a/tiledb/sm/cpp_api/fragment_info.h b/tiledb/sm/cpp_api/fragment_info.h
+  TILEDB_DEPRECATED
   void load(
       tiledb_encryption_type_t encryption_type,
       const std::string& encryption_key) const {
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer(
+      const std::string& name, T* buff, uint64_t nelements) {
     // Checks
     auto is_attr = schema_.has_attribute(name);
     auto is_dim = schema_.domain().has_dimension(name);
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer(
+      const std::string& name, std::vector<T>& buf) {
     return set_buffer(name, buf.data(), buf.size(), sizeof(T));
   }
 
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer(
+      const std::string& name, void* buff, uint64_t nelements) {
     // Checks
     auto is_attr = schema_.has_attribute(name);
     auto is_dim = schema_.domain().has_dimension(name);
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer(
       const std::string& name,
       uint64_t* offsets,
       uint64_t offset_nelements,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer(
       const std::string& name,
       uint64_t* offsets,
       uint64_t offset_nelements,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer(
       const std::string& name,
       std::vector<uint64_t>& offsets,
       std::vector<T>& data) {
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer(
       const std::string& name,
       std::pair<std::vector<uint64_t>, std::vector<T>>& buf) {
     // Checks
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer(
       const std::string& name,
       std::vector<uint64_t>& offsets,
       std::string& data) {
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer_nullable(
       const std::string& name,
       T* data,
       uint64_t data_nelements,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer_nullable(
       const std::string& name,
       std::vector<T>& buf,
       std::vector<uint8_t>& validity_bytemap) {
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer_nullable(
       const std::string& name,
       void* data,
       uint64_t data_nelements,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer_nullable(
       const std::string& name,
       uint64_t* offsets,
       uint64_t offset_nelements,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer_nullable(
       const std::string& name,
       std::vector<uint64_t>& offsets,
       std::vector<T>& data,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer_nullable(
       const std::string& name,
       std::tuple<std::vector<uint64_t>, std::vector<T>, std::vector<uint8_t>>&
           buf) {
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer_nullable(
       const std::string& name,
       std::vector<uint64_t>& offsets,
       std::string& data,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& get_buffer(
       const std::string& name,
       void** data,
       uint64_t* data_nelements,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& get_buffer(
       const std::string& name,
       uint64_t** offsets,
       uint64_t* offsets_nelements,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& get_buffer_nullable(
       const std::string& name,
       void** data,
       uint64_t* data_nelements,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& get_buffer_nullable(
       const std::string& name,
       uint64_t** offsets,
       uint64_t* offsets_nelements,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer(
       const std::string& attr,
       void* buff,
       uint64_t nelements,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer(
       const std::string& attr,
       uint64_t* offsets,
       uint64_t offset_nelements,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer_nullable(
       const std::string& attr,
       void* data,
       uint64_t data_nelements,
commit fb5efde4d30a31647a0edad927f02aad5cc48ffa
Date:   Fri Jul 9 11:37:29 2021 -0400
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_buffer_nullable(
       const std::string& attr,
       uint64_t* offsets,
       uint64_t offset_nelements,
commit 3b5128a2df7b5429da9f484622911dceb158b89b
Date:   Mon Jun 7 11:56:47 2021 -0400
diff --git a/tiledb/sm/cpp_api/array.h b/tiledb/sm/cpp_api/array.h
+  TILEDB_DEPRECATED
   static void consolidate(
       const Context& ctx,
       const std::string& uri,
commit 3b5128a2df7b5429da9f484622911dceb158b89b
Date:   Mon Jun 7 11:56:47 2021 -0400
diff --git a/tiledb/sm/cpp_api/array.h b/tiledb/sm/cpp_api/array.h
+  TILEDB_DEPRECATED
   static ArraySchema load_schema(
       const Context& ctx,
       const std::string& uri,
commit 3b5128a2df7b5429da9f484622911dceb158b89b
Date:   Mon Jun 7 11:56:47 2021 -0400
diff --git a/tiledb/sm/cpp_api/array.h b/tiledb/sm/cpp_api/array.h
+  TILEDB_DEPRECATED
   static void create(
       const std::string& uri,
       const ArraySchema& schema,
commit ad4e0b6b9cbbf93aa7f01b70730926b6ef79aa82
Date:   Fri Aug 6 07:30:55 2021 -0400
diff --git a/tiledb/sm/cpp_api/array.h b/tiledb/sm/cpp_api/array.h
+  TILEDB_DEPRECATED
   Array(
       const Context& ctx,
       const std::string& array_uri,
commit ad4e0b6b9cbbf93aa7f01b70730926b6ef79aa82
Date:   Fri Aug 6 07:30:55 2021 -0400
diff --git a/tiledb/sm/cpp_api/array.h b/tiledb/sm/cpp_api/array.h
+  TILEDB_DEPRECATED
   static void consolidate_metadata(
       const Context& ctx,
       const std::string& uri,

tags 2.4.0..2.5.0
-nothing-

tags 2.5.0..2.6.0
-nothing-

tags 2.6.0..2.7.0
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& add_range(
+      uint32_t dim_idx, T start, T end, T stride = 0) {
     impl::type_check<T>(schema_.domain().dimension(dim_idx).type());
     auto& ctx = ctx_.get();
     ctx.handle_error(tiledb_query_add_range(
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& add_range(
+      const std::string& dim_name, T start, T end, T stride = 0) {
     impl::type_check<T>(schema_.domain().dimension(dim_name).type());
     auto& ctx = ctx_.get();
     ctx.handle_error(tiledb_query_add_range_by_name(
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED
   Query& add_range(
       uint32_t dim_idx, const std::string& start, const std::string& end) {
     impl::type_check<char>(schema_.domain().dimension(dim_idx).type());
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED
   Query& add_range(
       const std::string& dim_name,
       const std::string& start,
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED
   uint64_t range_num(unsigned dim_idx) const {
     auto& ctx = ctx_.get();
     uint64_t range_num;
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED
   uint64_t range_num(const std::string& dim_name) const {
     auto& ctx = ctx_.get();
     uint64_t range_num;
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED std::array<T, 3> range(
+      unsigned dim_idx, uint64_t range_idx) {
     impl::type_check<T>(schema_.domain().dimension(dim_idx).type());
     auto& ctx = ctx_.get();
     const void *start, *end, *stride;
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED std::array<T, 3> range(
+      const std::string& dim_name, uint64_t range_idx) {
     impl::type_check<T>(schema_.domain().dimension(dim_name).type());
     auto& ctx = ctx_.get();
     const void *start, *end, *stride;
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED
   std::array<std::string, 2> range(unsigned dim_idx, uint64_t range_idx) {
     impl::type_check<char>(schema_.domain().dimension(dim_idx).type());
     auto& ctx = ctx_.get();
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED
   std::array<std::string, 2> range(
       const std::string& dim_name, uint64_t range_idx) {
     impl::type_check<char>(schema_.domain().dimension(dim_name).type());
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_subarray(const T* pairs, uint64_t size) {
     impl::type_check<T>(schema_.domain().type());
     auto& ctx = ctx_.get();
     if (size != schema_.domain().ndim() * 2) {
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_subarray(const Vec& pairs) {
     return set_subarray(pairs.data(), pairs.size());
   }
 
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_subarray(const std::initializer_list<T>& l) {
     return set_subarray(std::vector<T>(l));
   }
 
commit 5b30fcb0ba16bf3e4217688fb303452d08de0d47
Date:   Thu Nov 18 21:16:26 2021 -0500
diff --git a/tiledb/sm/cpp_api/query.h b/tiledb/sm/cpp_api/query.h
+  TILEDB_DEPRECATED Query& set_subarray(
+      const std::vector<std::array<T, 2>>& pairs) {
     std::vector<T> buf;
     buf.reserve(pairs.size() * 2);
     std::for_each(

tags 2.7.0..2.8.0-rc0
-nothing-
