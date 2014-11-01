CXX = g++
SRC_DIR = source/src
OBJ_DIR = source/obj
TEST_DIR = source/test
TEST_OBJ_DIR = source/test/obj
EXAMPLE_DIR = source/example
EXAMPLE_OBJ_DIR = source/example/obj
BIN_DIR = source/bin
HEAD_DIR = header
INCLUDE_PATHS = -I$(HEAD_DIR)
SRCS := $(wildcard $(SRC_DIR)/*.cc)
HEADS := $(wildcard $(HEAD_DIR)/*.h)
OBJS := $(patsubst $(SRC_DIR)/%.cc, $(OBJ_DIR)/%.o, $(SRCS))

.PHONY: clean all compile-all doc

all: compile-all examples

tests: tile_test storage_manager_test csv_file_test array_schema_test loader_test query_processor_test

examples: tile_example storage_manager_example csv_file_example array_schema_example loader_example query_processor_example

compile-all: $(OBJS)

# compile each main src file
# $< gets name of first matching dependency, $@ gets target name
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cc $(HEADS)
	$(CXX) $(INCLUDE_PATHS) -c $< -o $@

clean:
	rm -f $(OBJ_DIR)/* $(BIN_DIR)/* $(TEST_OBJ_DIR)/* $(EXAMPLE_OBJ_DIR)/*


####################
# Example Programs #
####################

# Compile each test and example src
# $< gets name of first matching dependency, $@ gets target name
$(TEST_OBJ_DIR)/%.o: $(TEST_DIR)/%.cc
	test -d $(TEST_OBJ_DIR) || mkdir -p $(TEST_OBJ_DIR)
	$(CXX) $(INCLUDE_PATHS) -c $< -o $@

$(EXAMPLE_OBJ_DIR)/%.o: $(EXAMPLE_DIR)/%.cc
	test -d $(EXAMPLE_OBJ_DIR) || mkdir -p $(EXAMPLE_OBJ_DIR)
	$(CXX) $(INCLUDE_PATHS) -c $< -o $@

# Link each test and example binary
$(BIN_DIR)/%: $(TEST_OBJ_DIR)/%.o $(OBJS)
	$(CXX) $(INCLUDE_PATHS) -o $@ $^

$(BIN_DIR)/%: $(EXAMPLE_OBJ_DIR)/%.o $(OBJS)
	$(CXX) $(INCLUDE_PATHS) -o $@ $^

# Tile test and example
tile_test: $(BIN_DIR)/tile_test
tile_example: $(BIN_DIR)/tile_example

# Storage manager and example
storage_manager_test: $(BIN_DIR)/storage_manager_test
storage_manager_example: $(BIN_DIR)/storage_manager_example

# CSV file test and example
csv_file_test: $(BIN_DIR)/csv_file_test
csv_file_example: $(BIN_DIR)/csv_file_example

# Array schema test and example
array_schema_test: $(BIN_DIR)/array_schema_test
array_schema_example: $(BIN_DIR)/array_schema_example

# Loader test and example
loader_test: $(BIN_DIR)/loader_test
loader_example: $(BIN_DIR)/loader_example

# Query processor test and example
query_processor_test: $(BIN_DIR)/query_processor_test
query_processor_example: $(BIN_DIR)/query_processor_example

#########################
# Documentation doxygen #
#########################
# Dynamically generates input source in case things change
# tracks everything in HEAD_DIR right now
doxyfile.inc: Makefile
	echo INPUT         =  doc/mainpage.dox $(HEADS) > doxyfile.inc
	echo FILE_PATTERNS =  *.h >> doxyfile.inc

# Generates html and latex documentation in doc/
doc: doxyfile.inc $(HEADS)
	doxygen Doxyfile.mk






# LIB_PATHS = /usr/local/lib/libspatialindex.so
# LIBS = -lpqxx -lpthread
