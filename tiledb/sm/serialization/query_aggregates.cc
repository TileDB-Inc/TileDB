/**
 * @file   query_aggregates.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file declares serialization functions for Query aggregates.
 */

#include "tiledb/sm/serialization/query_aggregates.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/readers/aggregators/operation.h"

namespace tiledb::sm::serialization {

#ifdef TILEDB_SERIALIZATION

void query_aggregates_to_capnp(
    Query& query, capnp::Query::Builder* query_builder) {
  // There is currently only a default channel
  size_t num_channels = 1;
  auto channels_builder = query_builder->initChannels(num_channels);
  auto channel_builder = channels_builder[0];
  channel_builder.setDefault(true);

  // Only 1 segment in the default channel currently
  size_t num_segments = 1;
  auto segments_builder = channel_builder.initSegments(num_segments);
  auto segment_builder = segments_builder[0];

  auto default_channel_aggregates = query.get_default_channel_aggregates();
  if (!default_channel_aggregates.empty()) {
    size_t num_aggregates = default_channel_aggregates.size();
    auto aggregates_builder = segment_builder.initAggregates(num_aggregates);
    size_t i = 0;
    for (const auto& agg : default_channel_aggregates) {
      auto aggregate_builder = aggregates_builder[i];
      aggregate_builder.setOutputFieldName(agg.first);
      aggregate_builder.setName(agg.second->aggregate_name());
      // TODO: Currently COUNT reports that its
      // field name is constants::count_of_rows. This causes deserialization
      // code to crash because an input field is always assumed to exist, so
      // deserialization calls some array schema functions on it. This should be
      // fixed in a followup work item by making the aggregators interface
      // return an optional field_name
      if (agg.second->aggregate_name() != constants::aggregate_count_str) {
        aggregate_builder.setInputFieldName(agg.second->field_name());
      }
      ++i;
    }
  }
}

void query_aggregates_from_capnp(
    const capnp::Query::Reader& query_reader, Query* const query) {
  // Should always be true as there is always at least 1 query default channel
  if (query_reader.hasChannels()) {
    auto channels_reader = query_reader.getChannels();
    // Only the query default channel is on the wire for now
    auto channel_reader = channels_reader[0];
    if (!channel_reader.getDefault()) {
      throw std::logic_error(
          "This channel is expected to be the default query channel");
    }
    // Should always be true because a channel has at least one segment
    if (channel_reader.hasSegments()) {
      auto segments_reader = channel_reader.getSegments();
      // Only a single segment is on the wire now until grouping operations
      // get implemented
      auto segment_reader = segments_reader[0];
      if (segment_reader.hasAggregates()) {
        auto aggregates_reader = segment_reader.getAggregates();
        for (const auto& aggregate : aggregates_reader) {
          if (aggregate.hasOutputFieldName() && aggregate.hasName()) {
            auto output_field = aggregate.getOutputFieldName();
            auto name = aggregate.getName();

            // TODO: Currently COUNT reports that its
            // field name is constants::count_of_rows. This causes
            // deserialization code to crash because an input field is always
            // assumed to exist, so deserialization calls some array schema
            // functions on it. This should be fixed in a followup work item by
            // making the aggregators interface return an optional field_name
            if (aggregate.hasInputFieldName()) {
              auto input_field = aggregate.getInputFieldName();
              auto& schema = query->array_schema();

              auto fi = tiledb::sm::FieldInfo(
                  input_field,
                  schema.var_size(input_field),
                  schema.is_nullable(input_field),
                  schema.cell_val_num(input_field),
                  schema.type(input_field));

              auto operation = Operation::make_operation(name, fi);
              query->add_aggregator_to_default_channel(
                  output_field, operation->aggregator());
            } else {
              auto operation = Operation::make_operation(name, nullopt);
              query->add_aggregator_to_default_channel(
                  output_field, operation->aggregator());
            }
          }
        }
      }
    }
  }
}

#endif  // TILEDB_SERIALIZATION

}  // namespace tiledb::sm::serialization