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
TILEDB_HEADS_DIR = tiledb/heads
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
DIST_HEADS_DIR = distributed/heads
DIST_SRC_DIR = distributed/src
DIST_OBJ_DIR = distributed/obj
DIST_BIN_DIR = distributed/bin
DOC_DIR = doc

# --- Paths --- #
CORE_HEADS_PATHS = -I$(CORE_HEADS_DIR)
TILEDB_HEADS_PATHS = -I$(TILEDB_HEADS_DIR)
DIST_HEADS_PATHS = -I$(DIST_HEADS_DIR)

# --- Files --- #
CORE_HEADS := $(wildcard $(CORE_HEADS_DIR)/*.h)
CORE_SRC := $(wildcard $(CORE_SRC_DIR)/*.cc)
CORE_OBJ := $(patsubst $(CORE_SRC_DIR)/%.cc, $(CORE_OBJ_DIR)/%.o, $(CORE_SRC))
TILEDB_HEADS := $(wildcard $(TILEDB_HEADS_DIR)/*.h)
TILEDB_SRC := $(wildcard $(TILEDB_SRC_DIR)/*.cc)
TILEDB_OBJ := $(patsubst $(TILEDB_SRC_DIR)/%.cc, $(TILEDB_OBJ_DIR)/%.o, $(TILEDB_SRC))
GTEST_INCLUDE := $(wildcard $(GTEST_INCLUDE_DIR)/*.h)
GTEST_OBJ := $(patsubst $(GTEST_SRC_DIR)/%.cc, $(GTEST_OBJ_DIR)/%.o, $(GTEST_SRC))
TEST_SRC := $(wildcard $(TEST_SRC_DIR)/*.cc)
TEST_OBJ := $(patsubst $(TEST_SRC_DIR)/%.cc, $(TEST_OBJ_DIR)/%.o, $(TEST_SRC))
DIST_SRC := $(wildcard $(DIST_SRC_DIR)/*.cc)
DIST_OBJ := $(patsubst $(DIST_SRC_DIR)/%.cc, $(DIST_OBJ_DIR)/%.o, $(DIST_SRC))
DIST_BIN := $(patsubst $(DIST_SRC_DIR)/%.cc, $(DIST_BIN_DIR)/%, $(DIST_SRC))

###################
# General Targets #
###################

.PHONY: core example gtest test doc doc_doxygen clean_core clean_gtest clean_test clean_tiledb clean_distributed clean

all: core
#all: core gtest test tiledb distributed doc

core: $(CORE_OBJ) 

tiledb: core $(TILEDB_BIN_DIR)/tiledb

gtest: $(GTEST_OBJ_DIR)/gtest-all.o

test: $(TEST_OBJ)

distributed: core $(DIST_OBJ)

doc: doxyfile.inc

clean: clean_core clean_gtest clean_test clean_tiledb clean_distributed

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
	$(CXX) $(TILEDB_HEADS_PATHS) $(CORE_HEADS_PATHS) -c $< -o $@
	@$(CXX) -MM $(TILEDB_HEADS_PATHS) $(CORE_HEADS_PATHS) $< > $(@:.o=.d)
	@mv -f $(@:.o=.d) $(@:.o=.d.tmp)
	@sed 's|.*:|$@:|' < $(@:.o=.d.tmp) > $(@:.o=.d)
	@rm -f $(@:.o=.d.tmp)

clean_tiledb:
	rm -f $(TILEDB_OBJ_DIR)/* $(TILEDB_BIN_DIR)/* 

# --- Linking --- #

$(TILEDB_BIN_DIR)/tiledb: $(TILEDB_OBJ) $(CORE_OBJ)
	@mkdir -p $(TILEDB_BIN_DIR)
	$(CXX) -fopenmp -o $@ $^
 
###############
# Distributed #
###############

# --- Compilation and dependency genration --- #

-include $(DIST_OBJ:.o=.d)

$(DIST_OBJ_DIR)/%.o: $(DIST_SRC_DIR)/%.cc
	@test -d $(DIST_OBJ_DIR) || mkdir -p $(DIST_OBJ_DIR)
	$(CXX) $(DIST_HEADS_PATHS)  $(CORE_HEADS_PATHS) -c $< -o $@
	@$(CXX) -MM $(CORE_HEADS_PATHS) $(DIST_HEADS_PATHS) $< > $(@:.o=.d)
	@mv -f $(@:.o=.d) $(@:.o=.d.tmp)
	@sed 's|.*:|$@:|' < $(@:.o=.d.tmp) > $(@:.o=.d)
	@rm -f $(@:.o=.d.tmp)

clean_distributed:
	rm -f $(DIST_OBJ_DIR)/* $(DIST_BIN_DIR)/* 

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
	@echo INPUT         =  $(DOC_DIR)/mainpage.dox $(DIST_HEADS) $(CORE_HEADS) > doxyfile.inc
	@echo FILE_PATTERNS =  *.h >> doxyfile.inc
	doxygen Doxyfile.mk

# LIB_PATHS = /usr/local/lib/libspatialindex.so
# LIBS = -lpqxx -lpthread
