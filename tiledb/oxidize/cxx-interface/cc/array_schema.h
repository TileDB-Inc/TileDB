#include "tiledb/oxidize/rust.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/array_schema/enumeration.h"

namespace tiledb::oxidize::sm {

using namespace tiledb::sm;

namespace attribute {

using ConstAttribute = const Attribute;

const std::string* enumeration_name_cxx(const Attribute& attribute);

}  // namespace attribute

namespace dimension {

using namespace tiledb::sm;

using ConstDimension = const tiledb::sm::Dimension;

void set_domain(Dimension& dimension, rust::Slice<const uint8_t> domain);
void set_tile_extent(Dimension& dimension, rust::Slice<const uint8_t> domain);

}  // namespace dimension

namespace enumeration {

using ConstEnumeration = const tiledb::sm::Enumeration;

static inline rust::Slice<const uint8_t> data_cxx(
    const Enumeration& enumeration) {
  std::span<const uint8_t> span = enumeration.data();
  return rust::Slice(span.data(), span.size());
}

static inline rust::Slice<const uint8_t> offsets_cxx(
    const Enumeration& enumeration) {
  std::span<const uint8_t> span = enumeration.offsets();
  return rust::Slice(span.data(), span.size());
}

}  // namespace enumeration

namespace array_schema {

struct MaybeEnumeration {
  std::optional<std::reference_wrapper<const std::string>> name_;
  std::shared_ptr<const tiledb::sm::Enumeration> value_;

  static MaybeEnumeration not_loaded(const std::string& enumeration_name) {
    return MaybeEnumeration{
        .name_ = std::optional(std::cref(enumeration_name)), .value_ = nullptr};
  }

  static MaybeEnumeration loaded(std::shared_ptr<const Enumeration> value) {
    return MaybeEnumeration{.name_ = std::nullopt, .value_ = value};
  }

  const std::string& name() const {
    if (name_.has_value()) {
      return name_.value().get();
    } else {
      return value_->name();
    }
  }

  std::shared_ptr<const Enumeration> get() const {
    return value_;
  }
};

std::unique_ptr<std::vector<MaybeEnumeration>> enumerations(
    const ArraySchema& schema);

}  // namespace array_schema

}  // namespace tiledb::oxidize::sm
