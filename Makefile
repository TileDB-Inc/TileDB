##########
# Macros #
##########

# --- Compiler --- #
CXX = g++

# --- Directories --- #
CORE_HEADS_DIR = core/heads
CORE_SRC_DIR = core/src
CORE_OBJ_DIR = core/obj
CORE_BIN_DIR = core/bin
TILEDB_SRC_DIR = tiledb/src
TILEDB_OBJ_DIR = tiledb/obj
TILEDB_BIN_DIR = tiledb/bin
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
CORE_HEADS_PATHS = -I$(CORE_HEADS_DIR)

# --- Files --- #
CORE_HEADS := $(wildcard $(CORE_HEADS_DIR)/*.h)
CORE_SRC := $(wildcard $(CORE_SRC_DIR)/*.cc)
CORE_OBJ := $(patsubst $(CORE_SRC_DIR)/%.cc, $(CORE_OBJ_DIR)/%.o, $(CORE_SRC))
TILEDB_SRC := $(wildcard $(TILEDB_SRC_DIR)/*.cc)
TILEDB_OBJ := $(patsubst $(TILEDB_SRC_DIR)/%.cc, $(TILEDB_OBJ_DIR)/%.o, $(TILEDB_SRC))
TILEDB_BIN := $(patsubst $(TILEDB_SRC_DIR)/%.cc, $(TILEDB_BIN_DIR)/%, $(TILEDB_SRC))
GTEST_INCLUDE := $(wildcard $(GTEST_INCLUDE_DIR)/*.h)
GTEST_OBJ := $(patsubst $(GTEST_SRC_DIR)/%.cc, $(GTEST_OBJ_DIR)/%.o, $(GTEST_SRC))
TEST_SRC := $(wildcard $(TEST_SRC_DIR)/*.cc)
TEST_OBJ := $(patsubst $(TEST_SRC_DIR)/%.cc, $(TEST_OBJ_DIR)/%.o, $(TEST_SRC))

###################
# General Targets #
###################

.PHONY: core example gtest test doc doc_doxygen clean_core clean_gtest clean_test clean_tiledb clean

all: core gtest test tiledb doc

core: $(CORE_OBJ) 

tiledb: $(TILEDB_BIN)

gtest: $(GTEST_OBJ_DIR)/gtest-all.o

test: $(TEST_OBJ)

doc: doxyfile.inc

clean: clean_core clean_gtest clean_test clean_tiledb

########
# Core #
########

# --- Compilation and dependency genration --- #

-include $(CORE_OBJ:.o=.d)

$(CORE_OBJ_DIR)/%.o: $(CORE_SRC_DIR)/%.cc
	@test -d $(CORE_OBJ_DIR) || mkdir -p $(CORE_OBJ_DIR)
	$(CXX) $(CORE_HEADS_PATHS) -c $< -o $@
	@$(CXX) -MM $(CORE_HEADS_PATHS) $< > $(@:.o=.d)
	@mv -f $(@:.o=.d) $(@:.o=.d.tmp)
	@sed 's|.*:|$@:|' < $(@:.o=.d.tmp) > $(@:.o=.d)
	@rm -f $(@:.o=.d.tmp)

clean_core:
	rm -f $(CORE_OBJ_DIR)/* $(CORE_BIN_DIR)/* 

##########
# TileDB #
##########

# --- Compilation and dependency genration --- #

-include $(TILEDB_OBJ:.o=.d)

$(TILEDB_OBJ_DIR)/%.o: $(TILEDB_SRC_DIR)/%.cc
	@test -d $(TILEDB_OBJ_DIR) || mkdir -p $(TILEDB_OBJ_DIR)
	$(CXX) $(CORE_HEADS_PATHS) -c $< -o $@
	@$(CXX) -MM $(CORE_HEADS_PATHS) $< > $(@:.o=.d)
	@mv -f $(@:.o=.d) $(@:.o=.d.tmp)
	@sed 's|.*:|$@:|' < $(@:.o=.d.tmp) > $(@:.o=.d)
	@rm -f $(@:.o=.d.tmp)

clean_tiledb:
	rm -f $(TILEDB_OBJ_DIR)/* $(TILEDB_BIN_DIR)/* 

# --- Linking --- #

$(TILEDB_BIN_DIR)/tiledb: $(TILEDB_OBJ_DIR)/tiledb.o \
 $(CORE_OBJ_DIR)/*.o
	@mkdir -p $(TILEDB_BIN_DIR)
	$(CXX) -fopenmp $(INCLUDE_PATHS) -o $@ $^

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

doxyfile.inc: $(CORE_HEADS)
	@echo INPUT         =  $(DOC_DIR)/mainpage.dox $(CORE_HEADS) > doxyfile.inc
	@echo FILE_PATTERNS =  *.h >> doxyfile.inc
	doxygen Doxyfile.mk

# LIB_PATHS = /usr/local/lib/libspatialindex.so
# LIBS = -lpqxx -lpthread
