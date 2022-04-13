#include "magic.h"

#include "tiledb/common/common.h"
#include "tiledb/sm/misc/types.h"

namespace tiledb {
namespace sm {

using tiledb::common::Status;

class magic_dict {
public:
  magic_dict();
  static int magic_load(magic_t magic);

private:  
  static void* uncompressed_magic_dict_;
  static shared_ptr<tiledb::sm::ByteVecValue> expanded_buffer_;
};

}  // namespace sm
}  // namespace tiledb
