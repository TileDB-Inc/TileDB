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

  arrayDirectory @8 :ArrayDirectory;
  # array directory (for reads)

  fragmentMetadataAll @9 :List(FragmentMetadata);
  # metadata for all fragments (for reads)

  openedAtEndTimestamp @10 :UInt64;
  # The ending timestamp that the array was last opened at
}

struct ArrayOpen {
  config @0 :Config;
  # Config
  queryType @1 :Text;
  # Query type to open the array for
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

    dimensionLabels @14 :List(DimensionLabel);
    # Dimension labels of the array

    enumerations @15: List(Enumeration);
    # Enumerations of the array

    enumerationPathMap @16: List(KV);
    # Enumeration name to path map
}

struct DimensionLabel {
# A label of a dimension
    dimensionId @0 :UInt32;
    # Index of the dimension the label is attached to

    name @1 :Text;
    # Name of the dimension label

    uri @2 :Text;
    # URI of the existing dimension label

    attributeName @3 :Text;
    # Name of the attribute that stores the label data

    order @4 :Text;
    # Order of the dimension label

    type @5 :Text;
    # Datatype of label data

    cellValNum @6 :UInt32;
    # Number of cells per label value

    external @7 :Bool;
    # Is label stored in array's label directory or externally

    relative @8 :Bool;
    # Is URI relative or absolute to array directory

    schema @9 :ArraySchema;
    # Label schema
}

struct ArraySchemaEvolution {
# Evolution of array schema
    attributesToDrop @0 :List(Text);
    # Attribute names to be dropped

    attributesToAdd @1 :List(Attribute);
    # Attributes to be added

    timestampRange @2 :List(UInt64);
    # Timestamp range of array schema

    enumerationsToAdd @3 :List(Enumeration);
    # Enumerations to be added

    enumerationsToDrop @4 :List(Text);
    # Enumeration names to be dropped
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

    order @7 :Text;
    # The prescribed order of the data stored in the attribute

    enumerationName @8 :Text;
    # Name of the enumeration for this attribute, if it has one
}

struct Enumeration {
# Enumeration of values for use by Attributes
    name @0 :Text;
    # Enumeration name

    pathName @1 :Text;
    # Enumeration path name

    type @2 :Text;
    # Type of the Enumeration values

    cellValNum @3 :UInt32;
    # Enumeration number of values per cell

    ordered @4 :Bool;
    # Whether the enumeration is considered orderable

    data @5 :Data;
    # The contents of the enumeration values

    offsets @6 :Data;
    # The contents of the enumeration offsets buffer
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

struct WebpConfig {
  quality @0 :Float32;
  # WebP lossless quality; Valid range from 0.0f-1.0f
  format @1 :UInt8;
  # WebP colorspace format.
  lossless @2 :Bool;
  # True if compression is lossless, false if lossy.
  extentX @3: UInt16;
  # Tile extent along X axis.
  extentY @4: UInt16;
  # Tile extent along Y axis.
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

  webpConfig @14 :WebpConfig;
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

struct UnorderedWriterState {
  isCoordsPass @0 :Bool;
  # Coordinate pass boolean for partial attribute write

  cellPos @1 : List(UInt64);
  # Cell positions for partial attribute writes

  coordDups @2 : List(UInt64);
  # Coordinate duplicates for partial attribute writes

  fragMeta @3 : FragmentMetadata;
  # Fragment metadata for partial attribute writes
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

  unorderedWriterState @7 :UnorderedWriterState;
  # Unordered writer state
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

struct LabelSubarrayRanges {
  # A set of label 1D ranges for a subarray

  dimensionId @0 :UInt32;
  # Index of the dimension the label is attached to

  name @1 :Text;
  # Name of the dimension label

  ranges @2 :SubarrayRanges;
  # A set of 1D ranges for a subarray
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

  labelRanges @4 :List(LabelSubarrayRanges);
  # List of 1D ranges for dimensions that have labels

  attributeRanges @5 :Map(Text, SubarrayRanges);
  # List of 1D ranges for each attribute

  coalesceRanges @6 :Bool = true;
  # True if Subarray should coalesce overlapping ranges.
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

  useEnumeration @3 :Bool;
  # Whether or not to use the associated attribute's Enumeration
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

  useEnumeration @6 :Bool;
  # Whether or not to use the associated attribute's Enumeration
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

    writtenBuffers @19 : List(Text);
    # written buffers for partial attribute writes
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

struct ArrayDirectory {
  # object representing an array directory

  struct TimestampedURI {
    uri @0 :Text;
    timestampStart @1 :UInt64;
    timestampEnd @2 :UInt64;
  }

  struct DeleteAndUpdateTileLocation {
    uri @0 :Text;
    conditionMarker @1 :Text;
    offset @2 :UInt64;
  }

  unfilteredFragmentUris @0 :List(Text);
  # fragment URIs

  consolidatedCommitUris @1 :List(Text);
  # consolidated commit URI set

  arraySchemaUris @2 :List(Text);
  # URIs of all the array schema files

  latestArraySchemaUri @3 :Text;
  # latest array schema URI.

  arrayMetaUrisToVacuum @4 :List(Text);
  # the array metadata files to vacuum

  arrayMetaVacUrisToVacuum @5 :List(Text);
  # the array metadata vac files to vacuum

  commitUrisToConsolidate @6 :List(Text);
  # the commit files to consolidate

  commitUrisToVacuum @7 :List(Text);
  # the commit files to vacuum

  consolidatedCommitUrisToVacuum @8 :List(Text);
  # the consolidated commit files to vacuum

  arrayMetaUris @9 :List(TimestampedURI);
  # the timestamped filtered array metadata URIs, after removing
  # the ones that need to be vacuumed and those that do not fall within
  # [timestamp_start, timestamp_end]

  fragmentMetaUris @10 :List(Text);
  # the URIs of the consolidated fragment metadata files

  deleteAndUpdateTileLocation @11 :List(DeleteAndUpdateTileLocation);
  # the location of delete tiles

  timestampStart @12 :UInt64;
   # Only the files created after timestamp_start are listed

  timestampEnd @13 :UInt64;
  # Only the files created before timestamp_end are listed
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
  arraySchemaLatest @0 :ArraySchema;
  # latest array schema

  arraySchemasAll @1 :Map(Text, ArraySchema);
  # map of all array schemas

  fragmentInfo @2 :List(SingleFragmentInfo);
  # information about fragments in the array

  toVacuum @3 :List(Text);
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
  struct GenericTileOffsets {
      rtree @0 :UInt64;
      # RTree serialized as a blob
      tileOffsets @1 :List(UInt64);
      # tile offsets
      tileVarOffsets @2 :List(UInt64);
      # variable tile offsets
      tileVarSizes @3 :List(UInt64);
      # sizes of the uncompressed variable tiles offsets
      tileValidityOffsets @4 :List(UInt64);
      # tile validity offsets
      tileMinOffsets @5 :List(UInt64);
      # min tile offsets
      tileMaxOffsets @6 :List(UInt64);
      # max tile offsets
      tileSumOffsets @7 :List(UInt64);
      # tile sum offsets
      tileNullCountOffsets @8 :List(UInt64);
      # null count offsets
      fragmentMinMaxSumNullCountOffset @9 :UInt64;
      # fragment min/max/sum/nullcount offsets
      processedConditionsOffsets @10 :UInt64;
      # processed conditions offsets
  }

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

  hasConsolidatedFooter @27 :Bool;
  # if the fragment metadata footer appears in a consolidated file

  gtOffsets @28 :GenericTileOffsets;
  # the start offsets of the generic tiles stored in the metadata file
}

struct MultiPartUploadState {
  partNumber@0 :UInt64;
  # The index of the next part in a multipart upload process

  uploadId@1 :Text;
  # S3 specific ID identifying a multipart upload process for a file

  status@2 :Text;
  # Status field used to signal an error in a multipart upload process

  completedParts@3 :List(CompletedPart);
  # A list of parts that are already uploaded

  bufferedChunks@4 :List(BufferedChunk);
  # S3 specific field. A partial remote global order write might not be
  # result in a direct multipart part upload, s3 does not permit parts to be
  # smaller than 5mb (except the last one). We buffer directly on S3 using
  # intermediate files until we can deliver a big enough multipart part.
  # This list helps us keep track of these intermediate buffering files.
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

struct BufferedChunk {
  uri@0 :Text;
  # path to intermediate chunk which buffers
  # a <5mb remote global order write operation

  size@1 :UInt64;
  # the size in bytes of the intermediate chunk
}
