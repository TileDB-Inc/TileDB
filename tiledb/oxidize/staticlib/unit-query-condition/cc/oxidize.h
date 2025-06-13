#ifndef TILEDB_OXIDIZE_UNIT_QUERY_CONDITION_H
#define TILEDB_OXIDIZE_UNIT_QUERY_CONDITION_H

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/query/ast/query_ast.h"
#include "tiledb/sm/query/readers/result_tile.h"

namespace tiledb::test {

using namespace tiledb::sm;

void instance_query_condition_datafusion_ffi(
    const tiledb::sm::ArraySchema& array_schema,
    const ResultTile& tile,
    const tiledb::sm::ASTNode& ast);

}  // namespace tiledb::test

#endif
