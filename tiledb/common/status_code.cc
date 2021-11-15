//
// Created by EH on 11/6/2021.
//

#include "status_code.h"

namespace tiledb::common {

std::string_view to_string_view(const StatusCode& sc) {
  switch (sc) {
    case StatusCode::Ok:
      return "Ok";
    case StatusCode::Error:
      return "Error";
    case StatusCode::StorageManager:
      return "[TileDB::StorageManager] Error";
    case StatusCode::FragmentMetadata:
      return "[TileDB::FragmentMetadata] Error";
    case StatusCode::ArraySchema:
      return "[TileDB::ArraySchema] Error";
    case StatusCode::Metadata:
      return "[TileDB::Metadata] Error";
    case StatusCode::IO:
      return "[TileDB::IO] Error";
    case StatusCode::Mem:
      return "[TileDB::Mem] Error";
    case StatusCode::GZip:
      return "[TileDB::GZip] Error";
    case StatusCode::Compression:
      return "[TileDB::Compression] Error";
    case StatusCode::Tile:
      return "[TileDB::Tile] Error";
    case StatusCode::TileIO:
      return "[TileDB::TileIO] Error";
    case StatusCode::Buffer:
      return "[TileDB::Buffer] Error";
    case StatusCode::Query:
      return "[TileDB::Query] Error";
    case StatusCode::ValidityVector:
      return "[TileDB::ValidityVector] Error";
    case StatusCode::VFS:
      return "[TileDB::VFS] Error";
    case StatusCode::ConstBuffer:
      return "[TileDB::ConstBuffer] Error";
    case StatusCode::Dimension:
      return "[TileDB::Dimension] Error";
    case StatusCode::Domain:
      return "[TileDB::Domain] Error";
    case StatusCode::Consolidator:
      return "[TileDB::Consolidator] Error";
    case StatusCode::LRUCache:
      return "[TileDB::LRUCache] Error";
    case StatusCode::KV:
      return "[TileDB::KV] Error";
    case StatusCode::KVItem:
      return "[TileDB::KVItem] Error";
    case StatusCode::KVIter:
      return "[TileDB::KVIter] Error";
    case StatusCode::Config:
      return "[TileDB::Config] Error";
    case StatusCode::Utils:
      return "[TileDB::Utils] Error";
    case StatusCode::FS_S3:
      return "[TileDB::S3] Error";
    case StatusCode::FS_HDFS:
      return "[TileDB::HDFS] Error";
    case StatusCode::Attribute:
      return "[TileDB::Attribute] Error";
    case StatusCode::WriteCellSlabIter:
      return "[TileDB::WriteCellSlabIter] Error";
    case StatusCode::Reader:
      return "[TileDB::Reader] Error";
    case StatusCode::Writer:
      return "[TileDB::Writer] Error";
    case StatusCode::PreallocatedBuffer:
      return "[TileDB::PreallocatedBuffer] Error";
    case StatusCode::Filter:
      return "[TileDB::Filter] Error";
    case StatusCode::Encryption:
      return "[TileDB::Encryption] Error";
    case StatusCode::Array:
      return "[TileDB::Array] Error";
    case StatusCode::VFSFileHandleError:
      return "[TileDB::VFSFileHandle] Error";
    case StatusCode::ContextError:
      return "[TileDB::Context] Error";
    case StatusCode::SubarrayError:
      return "[TileDB::Subarray] Error";
    case StatusCode::SubarrayPartitionerError:
      return "[TileDB::SubarrayPartitioner] Error";
    case StatusCode::RTreeError:
      return "[TileDB::RTree] Error";
    case StatusCode::CellSlabIterError:
      return "[TileDB::CellSlabIter] Error";
    case StatusCode::RestError:
      return "[TileDB::REST] Error";
    case StatusCode::SerializationError:
      return "[TileDB::Serialization] Error";
    case StatusCode::ThreadPoolError:
      return "[TileDB::ThreadPool] Error";
    case StatusCode::FragmentInfoError:
      return "[TileDB::FragmentInfo] Error";
    case StatusCode::DenseTilerError:
      return "[TileDB::DenseTiler] Error";
    case StatusCode::QueryConditionError:
      return "[TileDB::QueryCondition] Error";
    default:
      return "[TileDB::?] Error:";
  }
}

std::string to_string(const StatusCode& sc) {
  return std::string(to_string_view(sc));
}

}  // namespace tiledb::common
