#include "mgc_dict.h"

#include "tiledb/sm/compressors/util/gzip_wrappers.h"

namespace tiledb {
namespace sm {

static const char magic_mgc_compressed_bytes[] = {
#include "magic_mgc_gzipped.bin"  // TBD: will something complain about (usually to be) missing eol in included file?
};

shared_ptr<tiledb::sm::ByteVecValue> magic_dict::expanded_buffer_;

void* magic_dict::uncompressed_magic_dict_ = nullptr;

static magic_dict magic_dict_object;

magic_dict::magic_dict() {
  if (uncompressed_magic_dict_)
    return;

  expanded_buffer_ = make_shared<tiledb::sm::ByteVecValue>(HERE());
  if (!gzip_decompress(expanded_buffer_, reinterpret_cast<const uint8_t *>(&magic_mgc_compressed_bytes[0])).ok()) {
    throw std::exception("gzip_decompress failure!");
  }
  uncompressed_magic_dict_ = expanded_buffer_.get()->data();
}

int magic_dict::magic_load(magic_t magic) {
  void* data[1] = {uncompressed_magic_dict_};
  uint64_t sizes[1] = {expanded_buffer_->size()};
  // zero ok, non-zero error
  return magic_load_buffers(magic, &data[0], sizes, 1);
}

}  // namespace sm
}  // namespace tiledb
