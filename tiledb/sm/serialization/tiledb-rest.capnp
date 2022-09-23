@0xb57d9224b587d87f;

using Json = import "/capnp/compat/json.capnp";
using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("tiledb::sm::serialization::capnp");

# ** un-comment below for Go generator use **
#using Go = import "/go.capnp";
#$Go.package("capnp_models");
#$Go.import("capnp_models");

struct DomainArray {
  int8 @0 :List(Int8);
  uint8 @1 :List(UInt8);
  int16 @2 :List(Int16);
  uint16 @3 :List(UInt16);
  int32 @4 :List(Int32);
  uint32 @5 :List(UInt32);
  int64 @6 :List(Int64);
  uint64 @7 :List(UInt64);
  float32 @8 :List(Float32);
  float64 @9 :List(Float64);
}

struct KV {
  key @0 :Text;
  value @1 :Text;
}

struct Config {
# Represents a config object
  entries @0 :List(KV);
  # list of key-value settings
}

struct Array {
  endTimestamp @0 :UInt64;
  # ending timestamp array was opened

  queryType @1 :Text;
  # Array opened for query type

  uri @2 :Text;
  # Array uri

  startTimestamp @3 :UInt64;
  # starting timestamp array was opened

  arraySchemaLatest @4 :ArraySchema;
  # latest array schema

  arraySchemasAll @5 :Map(Text, ArraySchema);
  # map of all Array Schemas

  nonEmptyDomain @6 :NonEmptyDomainList;
  # non empty domain

  arrayMetadata @7 :ArrayMetadata;
  # array metadata
}

struct ArrayOpen {
  config @0 :Config;
  # Config
}

struct ArraySchema {
# ArraySchema during creation or retrieval
    arrayType @0 :Text;
    # Type of array

    attributes @1 :List(Attribute);
    # Attributes of array

    capacity @2 :UInt64;
    # Capacity of array

    cellOrder @3 :Text;
    # Order of cells

    coordsFilterPipeline @4 :FilterPipeline;
    # Type of compression for coordinates (enum)

    domain @5 :Domain;
    # Domain of array

    offsetFilterPipeline @6 :FilterPipeline;
    # Compression type of cell variable offsets (enum)

    tileOrder @7 :Text;
    # Tile order setting of array

    uri @8 :Text;
    # URI of schema

    version @9 :List(Int32);
    # file format version

    allowsDuplicates @10 :Bool;
    # True if the array allows coordinate duplicates.
    # Applicable only to sparse arrays.

    validityFilterPipeline @11 :FilterPipeline;
    # Type of compression for validity buffers (enum)

    name @12 :Text;
    # name of array schema

    timestampRange @13 :List(UInt64);
    # Timestamp range of array schema
}

struct ArraySchemaEvolution {
# Evolution of array schema
    attributesToDrop @0 :List(Text);
    # Attribute names to be dropped

    attributesToAdd @1 :List(Attribute);
    # Attributes to be added

    timestampRange @2 :List(UInt64);
    # Timestamp range of array schema
}

struct Attribute {
# Attribute of array
    cellValNum @0 :UInt32;
    # Attribute number of values per cell

    name @1 :Text;
    # Attribute name

    type @2 :Text;
    # TileDB attribute datatype

    filterPipeline @3 :FilterPipeline;
    # TileDB FilterPipeline for Attribute

    fillValue @4 :Data;
    # Default fill value

    nullable @5 :Bool;
    # Is attribute nullable

    fillValueValidity @6 :Bool;
    # Default validity fill value for nullable attributes
}

struct AttributeBufferHeader {
# Represents an attribute buffer header information

    name @0 :Text;
    # Attribute name

    fixedLenBufferSizeInBytes @1 :UInt64;
    # Number of bytes in the fixed-length attribute data buffer

    varLenBufferSizeInBytes @2 :UInt64;
    # Number of bytes in the var-length attribute data buffer

    validityLenBufferSizeInBytes @3 :UInt64;
    # Number of bytes in the validity data buffer

    originalFixedLenBufferSizeInBytes @4 :UInt64;
    # Original user set number of bytes in the fixed-length attribute data buffer

    originalVarLenBufferSizeInBytes @5 :UInt64;
    # Original user set number of bytes in the var-length attribute data buffer

    originalValidityLenBufferSizeInBytes @6 :UInt64;
    # Original user set number of bytes in the validity data buffer
}

struct Dimension {
# Dimension of array

    name @0 :Text;
    # Dimension name

    nullTileExtent @1 :Bool;
    # Is tile extent null

    type @2 :Text;
    # Datatype for Dimension

    tileExtent :union {
      int8 @3 :Int8;
      uint8 @4 :UInt8;
      int16 @5 :Int16;
      uint16 @6 :UInt16;
      int32 @7 :Int32;
      uint32 @8 :UInt32;
      int64 @9 :Int64;
      uint64 @10 :UInt64;
      float32 @11 :Float32;
      float64 @12 :Float64;
    }
    # Extent of tile

    domain @13 :DomainArray;
    # extent of domain

    filterPipeline @14 :FilterPipeline;
    # TileDB FilterPipeline for Dimension
}

struct Domain {
# Domain of array
    cellOrder @0 :Text;
    # Tile Order

    dimensions @1 :List(Dimension);
    # Array of dimensions

    tileOrder @2 :Text;
    # Tile Order

    type @3 :Text;
    # Datatype of domain
}

struct Error {
    code @0 :Int64;
    message @1 :Text;
}

struct FloatScaleConfig {
  scale @0 :Float64;
  offset @1 :Float64;
  byteWidth @2 :UInt64;
}

struct Filter {
  type @0 :Text;
  # filter type

  data :union {
    text @1 :Text;
    bytes @2 :Data;
    int8 @3 :Int8;
    uint8 @4 :UInt8;
    int16 @5 :Int16;
    uint16 @6 :UInt16;
    int32 @7 :Int32;
    uint32 @8 :UInt32;
    int64 @9 :Int64;
    uint64 @10 :UInt64;
    float32 @11 :Float32;
    float64 @12 :Float64;
  }
  # filter data

  floatScaleConfig @13 :FloatScaleConfig;
}

struct FilterPipeline {
  filters @0 :List(Filter);
}

struct Map(Key, Value) {
  entries @0 :List(Entry);
  struct Entry {
    key @0 :Key;
    value @1 :Value;
  }
}

struct MapUInt32 {
  entries @0 :List(Entry);
  struct Entry {
    key @0 :Text;
    value @1 :UInt32;
  }
}

struct MapInt64 {
  entries @0 :List(Entry);
  struct Entry {
    key @0 :Text;
    value @1 :Int64;
  }
}

struct MapUInt64 {
  entries @0 :List(Entry);
  struct Entry {
    key @0 :Text;
    value @1 :UInt64;
  }
}

struct MapFloat64 {
  entries @0 :List(Entry);
  struct Entry {
    key @0 :Text;
    value @1 :Float64;
  }
}

struct Stats {
# Stats struct

  timers @0 :MapFloat64;
  # timer

  counters @1 :MapUInt64;
  # counters
}

struct Writer {
  # Write struct
  checkCoordDups @0 :Bool;

  checkCoordOOB @1 :Bool;

  dedupCoords @2 :Bool;

  subarray @3 :DomainArray;
  # Old-style (single-range) subarray for dense writes

  subarrayRanges @4 :Subarray;
  # The query subarray/ranges object, new style range object

  stats @5 :Stats;
  # Stats object

  globalWriteStateV1 @6 :GlobalWriteState;
  # All the state necessary for global writes to work in TileDB Cloud
}

struct SubarrayRanges {
  # A set of 1D ranges for a subarray

  type @0 :Text;
  # Datatype of the ranges

  hasDefaultRange @1:Bool;
  # True if the range is the default range

  buffer @2 :Data;
  # The bytes of the ranges

  bufferSizes @3 :List(UInt64);
  # The list of sizes per range

  bufferStartSizes @4 :List(UInt64);
  # The list of start sizes per range
}

struct Subarray {
  # A Subarray

  layout @0 :Text;
  # The layout of the subarray

  ranges @1 :List(SubarrayRanges);
  # List of 1D ranges, one per dimension

  stats @2 :Stats;
  # Stats object

  relevantFragments @3 :List(UInt32);
  # Relevant fragments
}

struct SubarrayPartitioner {
  # The subarray partitioner

  struct PartitionInfo {
    subarray @0 :Subarray;
    start @1 :UInt64;
    end @2 :UInt64;
    splitMultiRange @3 :Bool;
  }

  struct State {
    start @0 :UInt64;
    end @1 :UInt64;
    singleRange @2 :List(Subarray);
    multiRange @3 :List(Subarray);
  }

  subarray @0 :Subarray;
  # The subarray the partitioner will iterate on to produce partitions.

  budget @1 :List(AttributeBufferSize);
  # Result size budget (in bytes) for all attributes.

  current @2 :PartitionInfo;
  # The current partition info

  state @3 :State;
  # The state information for the remaining partitions to be produced

  memoryBudget @4 :UInt64;
  # The memory budget for the fixed-sized attributes and the offsets of the var-sized attributes

  memoryBudgetVar @5 :UInt64;
  # The memory budget for the var-sized attributes

  memoryBudgetValidity @6 :UInt64;
  # The memory budget for the validity buffers

  stats @7 :Stats;
  # Stats object
}

struct ReadState {
  overflowed @0 :Bool;
  # `True` if the query produced results that could not fit in some buffer.

  unsplittable @1 :Bool;
  # True if the current subarray partition is unsplittable.

  initialized @2 :Bool;
  # True if the reader has been initialized

  subarrayPartitioner @3 :SubarrayPartitioner;
  # The subarray partitioner
}

struct ConditionClause {
  # A clause within a condition

  fieldName @0 :Text;
  # The name of the field this clause applies to

  value @1 :Data;
  # The comparison value

  op @2 :Text;
  # The comparison operation
}

struct ASTNode {
  # A representation of the AST representing a query condition
  isExpression @0 :Bool;
  # True if node is an expression/compound node

  # Value node fields
  fieldName @1 :Text;
  # The name of the field this clause applies to

  value @2 :Data;
  # The comparison value

  op @3 :Text;
  # The comparison operation

  # Expression node fields
  children @4 :List(ASTNode);
  # A list of children

  combinationOp @5 :Text;
  # The combination logical operator
}

struct Condition {
  # The query condition

  clauses @0 :List(ConditionClause);
  # All clauses in this condition

  clauseCombinationOps @1 :List(Text);
  # The operation that combines each condition

  tree @2 :ASTNode;
  # The AST representing this condition
}

struct QueryReader {
  # Read struct (can't be called reader due to class name conflict)

  layout @0 :Text;
  # The layout of the cells in the result of the subarray

  subarray @1 :Subarray;
  # The query subarray.

  readState @2 :ReadState;
  # Read state of reader

  condition @3 :Condition;
  # The query condition

  stats @4 :Stats;
  # Stats object
}

struct Delete {
  # Delete struct

  condition @0 :Condition;
  # The delete condition

  stats @1 :Stats;
  # Stats object
}

struct ResultCellSlab {
# Result cell slab

    fragIdx @0 :UInt32;
    # Fragment index

    tileIdx @1 :UInt64;
    # Tile index

    start @2 :UInt64;
    # Start of the cell slab

    length @3 :UInt64;
    # Length of the cell slab
}

struct FragmentIndex {
# Tile/cell index for a fragment

    tileIdx @0 :UInt64;
    # Tile index

    cellIdx @1 :UInt64;
    # Cell index
}

struct ReadStateIndex {
  resultCellSlab @0 :List(ResultCellSlab);
  # Result cell slab.

  fragTileIdx @1 :List(FragmentIndex);
  # Tile/cell index for each fragments.

  doneAddingResultTiles @2 :Bool;
  # Is the reader done adding result tiles.
}

struct ReaderIndex {
  # Reader struct for indexed readers.

  layout @0 :Text;
  # The layout of the cells in the result of the subarray

  subarray @1 :Subarray;
  # The query subarray.

  readState @2 :ReadStateIndex;
  # Read state of reader

  condition @3 :Condition;
  # The query condition

  stats @4 :Stats;
  # Stats object
}

struct Query {
    attributeBufferHeaders @0 :List(AttributeBufferHeader);
    # list of attribute buffer headers

    layout @1 :Text;
    # query write layout

    status @2 :Text;
    # query status

    type @3 :Text;
    # Type of query

    writer @4 :Writer;
    # writer contains data needed for continuation of global write order queries

    reader @5 :QueryReader;
    # reader contains data needed for continuation of incomplete reads

    array @6 :Array;
    # Represents an open array

    totalFixedLengthBufferBytes @7: UInt64;
    # Total number of bytes in fixed size attribute buffers

    totalVarLenBufferBytes @8: UInt64;
    # Total number of bytes in variable size attribute buffers

    totalValidityBufferBytes @9: UInt64;
    # Total number of bytes in validity buffers

    varOffsetsMode @10 :Text;
    # This field is not longer used, it is replaced by the config

    varOffsetsAddExtraElement @11 :Bool;
    # This field is not longer used, it is replaced by the config

    varOffsetsBitsize @12 :Int32;
    # This field is not longer used, it is replaced by the config

    config @13 :Config;
    # Config set on query

    stats @14 :Stats;
    # Stats object

    readerIndex @15 :ReaderIndex;
    # readerIndex contains data needed for continuation of incomplete sparse reads with index readers

    denseReader @16 :QueryReader;
    # denseReader contains data needed for continuation of incomplete dense reads with dense reader

    delete @17 :Delete;
    # delete contains QueryCondition representing deletion expression

    writtenFragmentInfo @18 :List(WrittenFragmentInfo);
    # Needed in global order writes when WrittenFragmentInfo gets updated
    # during finalize, but doesn't end up back on the client Query object
}

struct NonEmptyDomain {
  # object representing a non-empty domain

  nonEmptyDomain @0 :DomainArray;
  # Non-empty domain of array

  isEmpty @1 :Bool;
  # Is non-empty domain really empty?

  sizes @2 :List(UInt64);
  # Number of elements in DomainArray for var length
}

struct NonEmptyDomainList {
  # object representing non-empty domain for heterogeneous or string dimensions
  nonEmptyDomains @0 :List(NonEmptyDomain);
}

struct AttributeBufferSize {
  # object representing a buffer size of an attribute

  attribute @0: Text;
  # name of attribute

  offsetBytes @1: UInt64;
  # size (in bytes) of offset buffer

  dataBytes @2: UInt64;
  # size (in bytes) of data buffer

  validityBytes @3: UInt64;
  # size (in bytes) of data buffer
}

struct MaxBufferSizes {
  maxBufferSizes @0 :List(AttributeBufferSize);
  # a list of max buffer sizes, one per attribute
}

struct ArrayMetadata {
  # object representing array metadata

  struct MetadataEntry {
    key @0 :Text;
    type @1 :Text;
    valueNum @2 :UInt32;
    value @3 :Data;
    del @4 :Bool;
  }

  entries @0 :List(MetadataEntry);
  # list of metadata values
}

struct EstimatedResultSize {
  # object representing estimated
  struct ResultSize {
    # Result size
    sizeFixed @0 :Float64;
    sizeVar @1 :Float64;
    sizeValidity @2 :Float64;
  }

  struct MemorySize {
    # Memory Size
    sizeFixed @0 :UInt64;
    sizeVar @1 :UInt64;
    sizeValidity @2 :UInt64;
  }

  resultSizes @0 :Map(Text, ResultSize);
  memorySizes @1 :Map(Text, MemorySize);
}

struct FragmentInfoRequest {
  config @0 :Config;
  # Config
}

struct SingleFragmentInfo {
  arraySchemaName @0 :Text;
  # array schema name

  meta @1 :FragmentMetadata;
  # fragment metadata

  fragmentSize @2 : UInt64;
  # the size of the entire fragment directory
}

struct FragmentInfo {
  arraySchemasAll @0 :Map(Text, ArraySchema);
  # map of all array schemas

  fragmentInfo @1 :List(SingleFragmentInfo);
  # information about fragments in the array

  toVacuum @2 :List(Text);
  # the URIs of the fragments to vacuum
}

struct GroupMetadata {
  config @0 :Config;
  # Config

  metadata  @1 :ArrayMetadata;
  # metadata attached to group
}

struct GroupMember {
  uri @0 :Text;
  # URI of group Member

  type @1 :Text;
  # type of Member, group or array

  relative @2 :Bool;
  # is member URI relative to group

  name @3 :Text;
  # name of member, optional
}

struct Group {
  # Group

  struct GroupDetails {
    members @0 :List(GroupMember);
    # list of Members in group

    metadata  @1 :ArrayMetadata;
    # metadata attached to group
  }

  config @0 :Config;
  # Config

  group @1 :GroupDetails  $Json.name("group");
}

struct GroupUpdate {
  struct GroupUpdateDetails {
    membersToRemove @0 :List(Text) $Json.name("members_to_remove");
    # members to remove

    membersToAdd @1 :List(GroupMember) $Json.name("members_to_add");
    # members to add
  }

  config @0 :Config;
  # Config

  groupUpdate @1 :GroupUpdateDetails $Json.name("group_changes");
  # group update details
}

struct GroupCreate {
  # Create group details

  struct GroupCreateDetails {
  # details of a group

    uri @0 :Text;
    # URI where group should be created
  }

  config @0 :Config;
  # Config

  groupDetails @1 :GroupCreateDetails $Json.name("group_details");
}

struct GlobalWriteState {
  cellsWritten @0 :MapUInt64;
  # number of cells written for each attribute/dimension

  fragMeta @1 :FragmentMetadata;
  # metadata of the global write fragment

  lastCellCoords @2 :SingleCoord;
  # the last cell written;

  lastHilbertValue @3 :UInt64;
  # last hilbert value written

  multiPartUploadStates@4 :Map(Text, MultiPartUploadState);
  # A mapping of URIs to multipart upload states
  # Each file involved in a remote global order write (attr files,
  # offsets files, etc) is partially written as a multipart upload part
  # with each partial global order write operation (submit,
  # submit_and_finalize). This mapping makes the multipart upload info
  # available between partile global order write operations on the cloud side.
}

struct SingleCoord {
  coords @0 :List(List(UInt8));
  # coordinate data per dimension

  sizes @1 :List(UInt64);
  # sizes of data per dimension

  singleOffset @2 :List(UInt64);
  # offsets buffer for a var sized  attribute
}

struct FragmentMetadata {
  fileSizes @0 :List(UInt64);
  # The size of each attribute file

  fileVarSizes @1 :List(UInt64);
  # The size of each var attribute file

  fileValiditySizes @2 :List(UInt64);
  # The size of each validity attribute file

  fragmentUri @3 :Text;
  # The uri of the fragment this metadata belongs to

  hasTimestamps @4 :Bool;
  # True if the fragment has timestamps

  hasDeleteMeta @5 :Bool;
  # True if the fragment has delete metadata

  sparseTileNum @6 :UInt64;
  # The number of sparse tiles

  tileIndexBase@7 :UInt64;
  # Used to track the tile index base between global order writes

  tileOffsets @8 :List(List(UInt64));
  # Tile offsets in their attribute files

  tileVarOffsets @9 :List(List(UInt64));
  # Variable tile offsets in their attribute files

  tileVarSizes @10 :List(List(UInt64));
  # The sizes of the uncompressed variable tiles

  tileValidityOffsets @11 :List(List(UInt64));
  # Validity tile offests in their attribute files

  tileMinBuffer @12 :List(List(UInt8));
  # tile min buffers

  tileMinVarBuffer @13 :List(List(UInt8));
  # tile min buffers for var length data

  tileMaxBuffer @14 :List(List(UInt8));
  # tile max buffers

  tileMaxVarBuffer @15 :List(List(UInt8));
  # tile max buffers for var length data

  tileSums @16 :List(List(UInt8));
  # tile sum values

  tileNullCounts @17 :List(List(UInt64));
  # tile null count values

  fragmentMins @18 :List(List(UInt8));
  # fragment min values

  fragmentMaxs @19 :List(List(UInt8));
  # fragment max values

  fragmentSums @20 :List(UInt64);
  # fragment sum values

  fragmentNullCounts @21 :List(UInt64);
  # fragment null count values

  version @22 :UInt32;
  # the format version of this metadata

  timestampRange @23 :List(UInt64);
  # A pair of timestamps for fragment

  lastTileCellNum @24 :UInt64;
  # The number of cells in the last tile

  nonEmptyDomain @25 :NonEmptyDomainList;
  # The non empty domain of the fragment

  rtree @26 :Data;
  # The RTree for the MBRs serialized as a blob
}

struct MultiPartUploadState {
  partNumber@0 :UInt64;
  # The index of the next part in a multipart upload process

  uploadId@1 :Text;
  # S3 specific ID identifying a multipart upload process for a file

  bucket@2 :Text;
  # The S3 bucket name

  s3Key@3 :Text;
  # S3 specific multipart upload key

  status@4 :Text;
  # Status field used to signal an error in a multipart upload process

  completedParts@5 :List(CompletedPart);
  # A list of parts that are already uploaded
}
struct CompletedPart {
  eTag@0 :Text;
  # S3 specific hash for the uploaded part

  partNumber@1 :UInt64;
  # The index of the uploaded part
}

struct WrittenFragmentInfo {
  uri @0 :Text;
  timestampRange @1 :List(UInt64);
}
