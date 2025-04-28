#ifndef TILEDB_OXIDIZE_FILTER_H
#define TILEDB_OXIDIZE_FILTER_H

#include "tiledb/sm/filter/filter.h"

#include <type_traits>

namespace tiledb::oxidized {

using TraitObjectRepr = std::array<uintptr_t, 2>;
static constexpr TraitObjectRepr DROPPED_TRAIT_OBJECT = {0, 0};

struct BoxDynFilter {
 public:
  BoxDynFilter(BoxDynFilter&& movefrom) noexcept
      : repr_(movefrom.repr_) {
    movefrom.repr_ = DROPPED_TRAIT_OBJECT;
  }

  ~BoxDynFilter();

  using IsRelocatable = std::true_type;

  // TODO: trait methods

 private:
  TraitObjectRepr repr_;
};

using PtrBoxDynFilter = BoxDynFilter*;

}  // namespace tiledb::oxidized

namespace tiledb::sm {

class OxidizedFilter : public Filter {
 public:
  OxidizedFilter(FilterType type, Datatype input_data_type);

  Datatype output_datatype(Datatype input_type) const override;

  bool accepts_input_datatype(Datatype input_type) const override;

  void run_forward(
      const WriterTile& tile,
      WriterTile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  Status run_reverse(
      const Tile& tile,
      Tile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const override;

 protected:
  Filter* clone_impl() const override;

  std::ostream& output(std::ostream& os) const override;

 private:
  tiledb::oxidized::BoxDynFilter object_;
};

}  // namespace tiledb::sm

#endif

