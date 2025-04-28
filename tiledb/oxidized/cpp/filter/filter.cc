#include "tiledb/oxidized/cpp/filter/filter.h"
#include "cxxbridge/filter/src/lib.rs.h"
#include "tiledb/common/unreachable.h"

namespace tiledb::oxidized {

BoxDynFilter::~BoxDynFilter() {
  if (repr_ != DROPPED_TRAIT_OBJECT) {
    tiledb::oxidized::drop_trait_object_in_place(this);
  }
}

}  // namespace tiledb::oxidized

namespace tiledb::sm {

static tiledb::oxidized::BoxDynFilter create_filter(FilterType type, Datatype) {
  switch (type) {
    default:
      stdx::unreachable();
  }
}

OxidizedFilter::OxidizedFilter(FilterType type, Datatype input_data_type)
    : Filter(type, input_data_type)
    , object_(std::move(create_filter(type, input_data_type))) {
}

Datatype OxidizedFilter::output_datatype(Datatype) const {
  // TODO
  stdx::unreachable();
}

bool OxidizedFilter::accepts_input_datatype(Datatype) const {
  // TODO
  stdx::unreachable();
}

void OxidizedFilter::run_forward(
    const WriterTile&,
    WriterTile* const,
    FilterBuffer*,
    FilterBuffer*,
    FilterBuffer*,
    FilterBuffer*) const {
  // TODO
  stdx::unreachable();
}

Status OxidizedFilter::run_reverse(
    const Tile&,
    Tile* const,
    FilterBuffer*,
    FilterBuffer*,
    FilterBuffer*,
    FilterBuffer*,
    const Config&) const {
  // TODO
  stdx::unreachable();
}

Filter* OxidizedFilter::clone_impl() const {
  // TODO
  stdx::unreachable();
}

std::ostream& OxidizedFilter::output(std::ostream&) const {
  // TODO
  stdx::unreachable();
}

}  // namespace tiledb::sm

