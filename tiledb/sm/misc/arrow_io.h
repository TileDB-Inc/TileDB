#include <memory>

#include <tiledb/sm/cpp_api/tiledb>
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace arrow {

class ArrowImporter;
class ArrowExporter;

class ArrowAdapter {
public:
  ArrowAdapter(std::shared_ptr<tiledb::Query> query);
  ~ArrowAdapter();

  void export_buffer(const char* name, void** arrow_schema, void** arrow_array);
  void import_buffer(const char* name, void* arrow_schema, void* arrow_array);

private:
  ArrowImporter* importer_;
  ArrowExporter* exporter_;
};

TILEDB_EXPORT
tiledb::sm::Status query_get_buffer_arrow_array(
    std::shared_ptr<Query> query,
    std::string name,
    void** arrow_schema,
    void** arrow_array);

TILEDB_EXPORT
tiledb::sm::Status query_set_buffer_arrow_array(
    std::shared_ptr<Query> query,
    std::string name,
    void* arrow_schema,
    void* arrow_array);

};  // end namespace arrow
};  // end namespace tiledb