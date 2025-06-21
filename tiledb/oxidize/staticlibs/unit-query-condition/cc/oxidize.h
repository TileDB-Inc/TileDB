#ifndef TILEDB_OXIDIZE_UNIT_QUERY_CONDITION_H
#define TILEDB_OXIDIZE_UNIT_QUERY_CONDITION_H

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/query/ast/query_ast.h"
#include "tiledb/sm/query/readers/result_tile.h"

namespace tiledb::test::query_condition_datafusion {

using namespace tiledb::sm;

std::shared_ptr<ArraySchema> example_schema();

std::unique_ptr<std::vector<uint8_t>> instance_ffi(
    const tiledb::sm::ArraySchema& array_schema,
    const ResultTile& tile,
    const tiledb::sm::ASTNode& ast);

}  // namespace tiledb::test::query_condition_datafusion

#endif
