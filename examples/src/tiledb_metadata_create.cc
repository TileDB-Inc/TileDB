/*
 * File: tiledb_metadata_create.cc
 * 
 * Creating a TileDB metadata structure. 
 */

#include "tiledb.h"

int main() {
  /* Intialize context. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx);

 /*
  * Define the metadata schema struct.
  * IMPORTANT: Initialize it as empty, so that all numeric members get set
  * to 0, and all pointers to NULL. This is vital so that TileDB can set
  * default values for the members not set by the user.
  */
  TileDB_MetadataSchema metadata_schema = {};

  /* Set metadata name to "meta", inside (existing) array "my_workspace/A". */
  metadata_schema.metadata_name_ = "my_workspace/A/meta";

  /* Set attributes and number of attributes. */
  int attribute_num = 2;
  metadata_schema.attributes_ = new const char*[attribute_num];
  metadata_schema.attributes_[0] = "a1";
  metadata_schema.attributes_[1] = "a2";
  metadata_schema.attribute_num_ = attribute_num;

  /* Set types: **int32:3** for "a1", **float32:var** for "a2" */
  metadata_schema.types_ = new const char*[attribute_num]; 
  metadata_schema.types_[0] = "int32:3";
  metadata_schema.types_[1] = "float32:var";

  /* 
   * NOTE: The rest of the array schema members will be set to default values.
   * This implies that there will be no compression, and that the consolidation
   * step will be set to 1.
   */

  /* Create the metadata. */
  tiledb_metadata_create(tiledb_ctx, &metadata_schema); 

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  /* Clean up. */
  delete [] metadata_schema.attributes_;
  delete [] metadata_schema.types_;

  return 0;
}
