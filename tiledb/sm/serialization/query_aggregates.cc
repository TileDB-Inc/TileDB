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

void query_channels_to_capnp(
    Query& query, capnp::Query::Builder* query_builder) {
  // Here, don't serialize if we don't have channels or if the default channel
  // has no aggregates on it.
  auto channels = query.get_channels();
  if (channels.empty() || (channels.size() == 1 && channels[0].is_default() &&
                           channels[0].aggregates().empty())) {
    return;
  }
  auto channels_builder = query_builder->initChannels(channels.size());
  for (const auto& channel : channels) {
    auto channel_builder = channels_builder[0];
    channel_builder.setDefault(channel.is_default());

    auto& aggregates = channel.aggregates();
    if (aggregates.empty()) {
      continue;
    }

    auto aggregates_builder = channel_builder.initAggregates(aggregates.size());
    size_t i = 0;
    for (const auto& agg : aggregates) {
      auto aggregate_builder = aggregates_builder[i];
      aggregate_builder.setOutputFieldName(agg.first);
      aggregate_builder.setName(agg.second->aggregate_name());
      // TODO: Currently COUNT reports that its field name is
      // constants::count_of_rows. This causes deserialization code to crash
      // because an input field is always assumed to exist, so deserialization
      // calls some array schema functions on it. This should be fixed in a
      // followup work item by making the aggregators interface return an
      // optional field_name
      if (agg.second->aggregate_name() != constants::aggregate_count_str) {
        aggregate_builder.setInputFieldName(agg.second->field_name());
      }
      ++i;
    }
  }
}

void query_channels_from_capnp(
    const capnp::Query::Reader& query_reader, Query* const query) {
  // We might not have channels if the default channel had no aggregates.
  if (query_reader.hasChannels()) {
    auto channels_reader = query_reader.getChannels();
    // Only the query default channel is on the wire for now
    for (size_t i = 0; i < channels_reader.size(); ++i) {
      auto channel_reader = channels_reader[i];

      if (channel_reader.hasAggregates()) {
        LegacyQueryAggregatesOverDefault::ChannelAggregates aggregates;

        auto aggregates_reader = channel_reader.getAggregates();
        for (const auto& aggregate : aggregates_reader) {
          if (aggregate.hasOutputFieldName() && aggregate.hasName()) {
            auto output_field = aggregate.getOutputFieldName();
            auto name = aggregate.getName();

            // TODO: Currently COUNT reports that its field name is
            // constants::count_of_rows. This causes deserialization code to
            // crash because an input field is always assumed to exist, so
            // deserialization calls some array schema functions on it. This
            // should be fixed in a followup work item by making the aggregators
            // interface return an optional field_name
            std::optional<FieldInfo> fi;
            if (aggregate.hasInputFieldName()) {
              auto input_field = aggregate.getInputFieldName();
              auto& schema = query->array_schema();

              fi.emplace(tiledb::sm::FieldInfo(
                  input_field,
                  schema.var_size(input_field),
                  schema.is_nullable(input_field),
                  schema.cell_val_num(input_field),
                  schema.type(input_field)));
            }

            auto operation = Operation::make_operation(name, fi);
            aggregates.emplace(output_field, operation->aggregator());
          }
        }
        if (!aggregates.empty()) {
          query->add_channel(LegacyQueryAggregatesOverDefault{
              channel_reader.getDefault(), aggregates});
        }
      }
    }
  }
}

#endif  // TILEDB_SERIALIZATION

}  // namespace tiledb::sm::serialization
