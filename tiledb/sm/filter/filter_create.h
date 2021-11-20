//
// Created by EH on 11/21/2021.
//

#ifndef TILEDB_FILTER_CREATE_H
#define TILEDB_FILTER_CREATE_H

#include "filter.h"

namespace tiledb::sm {

class FilterCreate {
 public:
  /**
   * Factory method to make a new Filter instance of the given type.
   *
   * @param type Filter type to make
   * @return New Filter instance or nullptr on error.
   */
  static tiledb::sm::Filter* make(tiledb::sm::FilterType type);

  /**
   * Deserializes a new Filter instance from the data in the given buffer.
   *
   * @param buff The buffer to deserialize from.
   * @param filter New filter instance (caller's responsibility to free).
   * @return Status
   */
  static Status deserialize(ConstBuffer* buff, Filter** filter);
};

}  // namespace tiledb::sm

#endif  // TILEDB_FILTER_CREATE_H
