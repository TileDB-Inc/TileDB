#include <assert.h>
#include <stdlib.h>
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_serialization.h>

#include <test/support/tdb_catch.h>

static constexpr std::string_view schema_str = R"rstr(
{"arrayType":"dense","attributes":[{
		"cellValNum":1,"compressor":"NO_COMPRESSION",
		"compressorLevel":-1,"name":"a1","type":"INT32"}],
		"capacity":"10000","cellOrder":"row-major",
		"coordsCompression":"ZSTD","coordsCompressionLevel":-1,
		"domain":{"cellOrder":"row-major","dimensions":[{"name":
		"d1","nullTileExtent":false,"type":"INT64","tileExtent":{
		"int64":"5"},"domain":{"int64":["0","99"]}}],
		"tileOrder":"row-major","type":"INT64"},
		"offsetCompression":"ZSTD","offsetCompressionLevel":-1,
		"tileOrder":"row-major", "version":[1,3,0]}
)rstr";

TEST_CASE(
    "Capnp serialization: Filter pipeline default construction (sc-18250)",
    "[serialization]") {
  int32_t status;
  tiledb_ctx_t* ctx;
  tiledb_buffer_t* buf;
  tiledb_array_schema_t* schema;

  status = tiledb_ctx_alloc(NULL, &ctx);
  REQUIRE(status == TILEDB_OK);

  status = tiledb_buffer_alloc(ctx, &buf);
  REQUIRE(status == TILEDB_OK);

  status = tiledb_buffer_set_data(
      ctx, buf, (void*)schema_str.data(), schema_str.size());
  REQUIRE(status == TILEDB_OK);

  status = tiledb_deserialize_array_schema(ctx, buf, TILEDB_JSON, 0, &schema);
  REQUIRE(status == TILEDB_OK);
}
