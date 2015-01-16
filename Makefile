##########
# Macros #
##########

# --- Compiler --- #
CXX = g++

# --- Directories --- #
CORE_INCLUDE_DIR = core/include
CORE_SRC_DIR = core/src
CORE_OBJ_DIR = core/obj
CORE_BIN_DIR = core/bin
EXAMPLE_SRC_DIR = example/src
EXAMPLE_OBJ_DIR = example/obj
EXAMPLE_BIN_DIR = example/bin
GTEST_DIR = gtest
GTEST_INCLUDE_DIR = gtest/include
GTEST_SRC_DIR = gtest/src
GTEST_OBJ_DIR = gtest/obj
GTEST_BIN_DIR = gtest/bin
TEST_SRC_DIR = test/src
TEST_OBJ_DIR = test/obj
TEST_BIN_DIR = test/bin
DOC_DIR = doc

# --- Paths --- #
CORE_INCLUDE_PATHS = -I$(CORE_INCLUDE_DIR)

# --- Files --- #
CORE_INCLUDE := $(wildcard $(CORE_INCLUDE_DIR)/*.h)
CORE_SRC := $(wildcard $(CORE_SRC_DIR)/*.cc)
CORE_OBJ := $(patsubst $(CORE_SRC_DIR)/%.cc, $(CORE_OBJ_DIR)/%.o, $(CORE_SRC))
EXAMPLE_SRC := $(wildcard $(EXAMPLE_SRC_DIR)/*.cc)
EXAMPLE_OBJ := $(patsubst $(EXAMPLE_SRC_DIR)/%.cc, $(EXAMPLE_OBJ_DIR)/%.o, $(EXAMPLE_SRC))
EXAMPLE_BIN := $(patsubst $(EXAMPLE_SRC_DIR)/%.cc, $(EXAMPLE_BIN_DIR)/%, $(EXAMPLE_SRC))
GTEST_INCLUDE := $(wildcard $(GTEST_INCLUDE_DIR)/*.h)
GTEST_OBJ := $(patsubst $(GTEST_SRC_DIR)/%.cc, $(GTEST_OBJ_DIR)/%.o, $(GTEST_SRC))
TEST_SRC := $(wildcard $(TEST_SRC_DIR)/*.cc)
TEST_OBJ := $(patsubst $(TEST_SRC_DIR)/%.cc, $(TEST_OBJ_DIR)/%.o, $(TEST_SRC))

###################
# General Targets #
###################

.PHONY: core example gtest test doc doc_doxygen clean_core clean_example clean_gtest clean_test clean

all: core example gtest test doc

core: $(CORE_OBJ)

example: $(EXAMPLE_BIN)

gtest: $(GTEST_OBJ_DIR)/gtest-all.o

test: $(TEST_OBJ)

doc: doxyfile.inc

clean: clean_core clean_example clean_gtest clean_test

###############
# Core TileDB #
###############

# --- Compilation and dependency genration --- #

-include $(CORE_OBJ:.o=.d)

$(CORE_OBJ_DIR)/%.o: $(CORE_SRC_DIR)/%.cc
	@test -d $(CORE_OBJ_DIR) || mkdir -p $(CORE_OBJ_DIR)
	$(CXX) $(CORE_INCLUDE_PATHS) -c $< -o $@
	@$(CXX) -MM $(CORE_INCLUDE_PATHS) $< > $(@:.o=.d)
	@mv -f $(@:.o=.d) $(@:.o=.d.tmp)
	@sed 's|.*:|$@:|' < $(@:.o=.d.tmp) > $(@:.o=.d)
	@rm -f $(@:.o=.d.tmp)

clean_core:
	rm -f $(CORE_OBJ_DIR)/* $(CORE_BIN_DIR)/* 

############
# Examples #
############

# --- Compilation and dependency genration --- #

-include $(EXAMPLE_OBJ:.o=.d)

$(EXAMPLE_OBJ_DIR)/%.o: $(EXAMPLE_SRC_DIR)/%.cc
	@test -d $(EXAMPLE_OBJ_DIR) || mkdir -p $(EXAMPLE_OBJ_DIR)
	$(CXX) $(CORE_INCLUDE_PATHS) -c $< -o $@
	@$(CXX) -MM $(CORE_INCLUDE_PATHS) $< > $(@:.o=.d)
	@mv -f $(@:.o=.d) $(@:.o=.d.tmp)
	@sed 's|.*:|$@:|' < $(@:.o=.d.tmp) > $(@:.o=.d)
	@rm -f $(@:.o=.d.tmp)

clean_example:
	rm -f $(EXAMPLE_OBJ_DIR)/* $(EXAMPLE_BIN_DIR)/* 

# --- Linking --- #

$(EXAMPLE_BIN_DIR)/example_array_schema: $(EXAMPLE_OBJ_DIR)/example_array_schema.o \
 $(CORE_OBJ_DIR)/array_schema.o $(CORE_OBJ_DIR)/hilbert_curve.o \
 $(CORE_OBJ_DIR)/tile.o $(CORE_OBJ_DIR)/csv_file.o
	@test -d $(EXAMPLE_BIN_DIR) || mkdir -p $(EXAMPLE_BIN_DIR)
	$(CXX) $(INCLUDE_PATHS) -o $@ $^

$(EXAMPLE_BIN_DIR)/example_csv_file: $(EXAMPLE_OBJ_DIR)/example_csv_file.o \
 $(CORE_OBJ_DIR)/csv_file.o $(CORE_OBJ_DIR)/tile.o
	@test -d $(EXAMPLE_BIN_DIR) || mkdir -p $(EXAMPLE_BIN_DIR)
	$(CXX) $(INCLUDE_PATHS) -o $@ $^

$(EXAMPLE_BIN_DIR)/example_loader: $(EXAMPLE_OBJ_DIR)/example_loader.o \
 $(CORE_OBJ_DIR)/loader.o $(CORE_OBJ_DIR)/tile.o $(CORE_OBJ_DIR)/array_schema.o \
 $(CORE_OBJ_DIR)/csv_file.o $(CORE_OBJ_DIR)/storage_manager.o $(CORE_OBJ_DIR)/hilbert_curve.o
	@test -d $(EXAMPLE_BIN_DIR) || mkdir -p $(EXAMPLE_BIN_DIR)
	$(CXX) $(INCLUDE_PATHS) -o $@ $^

$(EXAMPLE_BIN_DIR)/example_query_processor: $(EXAMPLE_OBJ_DIR)/example_query_processor.o \
 $(CORE_OBJ_DIR)/query_processor.o $(CORE_OBJ_DIR)/tile.o $(CORE_OBJ_DIR)/array_schema.o \
 $(CORE_OBJ_DIR)/csv_file.o $(CORE_OBJ_DIR)/loader.o $(CORE_OBJ_DIR)/storage_manager.o   \
 $(CORE_OBJ_DIR)/hilbert_curve.o
	@test -d $(EXAMPLE_BIN_DIR) || mkdir -p $(EXAMPLE_BIN_DIR)
	$(CXX) $(INCLUDE_PATHS) -o $@ $^

$(EXAMPLE_BIN_DIR)/example_storage_manager: $(EXAMPLE_OBJ_DIR)/example_storage_manager.o \
 $(CORE_OBJ_DIR)/storage_manager.o $(CORE_OBJ_DIR)/tile.o $(CORE_OBJ_DIR)/array_schema.o \
 $(CORE_OBJ_DIR)/csv_file.o $(CORE_OBJ_DIR)/hilbert_curve.o
	@test -d $(EXAMPLE_BIN_DIR) || mkdir -p $(EXAMPLE_BIN_DIR)
	$(CXX) $(INCLUDE_PATHS) -o $@ $^

$(EXAMPLE_BIN_DIR)/example_tile: $(EXAMPLE_OBJ_DIR)/example_tile.o \
 $(CORE_OBJ_DIR)/tile.o $(CORE_OBJ_DIR)/csv_file.o
	@test -d $(EXAMPLE_BIN_DIR) || mkdir -p $(EXAMPLE_BIN_DIR)
	$(CXX) $(INCLUDE_PATHS) -o $@ $^


###############
# Google test #
###############

$(GTEST_OBJ_DIR)/gtest-all.o: gtest/src/gtest-all.cc $(wildcard gtest/include/gtest/*.h)
	@test -d $(GTEST_OBJ_DIR) || mkdir -p $(GTEST_OBJ_DIR)
	$(CXX) -isystem $(GTEST_INCLUDE_DIR) -I$(GTEST_DIR) -pthread -c $< -o $@

clean_gtest:
	rm -f $(GTEST_OBJ_DIR)/* $(GTEST_BIN_DIR)/* 

#########
# Tests #
#########

# Coming up soon...

#########################
# Documentation doxygen #
#########################

doxyfile.inc: $(CORE_INCLUDE)
	@echo INPUT         =  $(DOC_DIR)/mainpage.dox $(CORE_INCLUDE) > doxyfile.inc
	@echo FILE_PATTERNS =  *.h >> doxyfile.inc
	doxygen Doxyfile.mk

# LIB_PATHS = /usr/local/lib/libspatialindex.so
# LIBS = -lpqxx -lpthread
