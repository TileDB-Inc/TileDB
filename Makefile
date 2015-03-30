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
TILEDB_INCLUDE_DIR = tiledb/include
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
LA_INCLUDE_DIR = la/include
LA_SRC_DIR = la/src
LA_OBJ_DIR = la/obj
LA_BIN_DIR = la/bin
DOC_DIR = doc

# --- Paths --- #
CORE_INCLUDE_PATHS = -I$(CORE_INCLUDE_DIR)
TILEDB_INCLUDE_PATHS = -I$(TILEDB_INCLUDE_DIR)
LA_INCLUDE_PATHS = -I$(LA_INCLUDE_DIR)

# --- Files --- #
CORE_INCLUDE := $(wildcard $(CORE_INCLUDE_DIR)/*.h)
CORE_SRC := $(wildcard $(CORE_SRC_DIR)/*.cc)
CORE_OBJ := $(patsubst $(CORE_SRC_DIR)/%.cc, $(CORE_OBJ_DIR)/%.o, $(CORE_SRC))
TILEDB_INCLUDE := $(wildcard $(TILEDB_INCLUDE_DIR)/*.h)
TILEDB_SRC := $(wildcard $(TILEDB_SRC_DIR)/*.cc)
TILEDB_OBJ := $(patsubst $(TILEDB_SRC_DIR)/%.cc, $(TILEDB_OBJ_DIR)/%.o, $(TILEDB_SRC))
GTEST_INCLUDE := $(wildcard $(GTEST_INCLUDE_DIR)/*.h)
GTEST_OBJ := $(patsubst $(GTEST_SRC_DIR)/%.cc, $(GTEST_OBJ_DIR)/%.o, $(GTEST_SRC))
TEST_SRC := $(wildcard $(TEST_SRC_DIR)/*.cc)
TEST_OBJ := $(patsubst $(TEST_SRC_DIR)/%.cc, $(TEST_OBJ_DIR)/%.o, $(TEST_SRC))
LA_SRC := $(wildcard $(LA_SRC_DIR)/*.cc)
LA_OBJ := $(patsubst $(LA_SRC_DIR)/%.cc, $(LA_OBJ_DIR)/%.o, $(LA_SRC))
LA_BIN := $(patsubst $(LA_SRC_DIR)/%.cc, $(LA_BIN_DIR)/%, $(LA_SRC))

###################
# General Targets #
###################

.PHONY: core example gtest test doc doc_doxygen clean_core clean_gtest \
 clean_test clean_tiledb clean_la clean

all: core gtest test tiledb la doc

core: $(CORE_OBJ) 

tiledb: core $(TILEDB_BIN_DIR)/tiledb

gtest: $(GTEST_OBJ_DIR)/gtest-all.o

test: $(TEST_OBJ)

la: core $(LA_OBJ) $(LA_BIN_DIR)/example_transpose

doc: doxyfile.inc

clean: clean_core clean_gtest clean_test clean_tiledb clean_la

########
# Core #
########

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

##########
# TileDB #
##########

# --- Compilation and dependency genration --- #

-include $(TILEDB_OBJ:.o=.d)

$(TILEDB_OBJ_DIR)/%.o: $(TILEDB_SRC_DIR)/%.cc
	@test -d $(TILEDB_OBJ_DIR) || mkdir -p $(TILEDB_OBJ_DIR)
	$(CXX) $(TILEDB_INCLUDE_PATHS) $(CORE_INCLUDE_PATHS) -c $< -o $@
	@$(CXX) -MM $(TILEDB_INCLUDE_PATHS) $(CORE_INCLUDE_PATHS) $< > $(@:.o=.d)
	@mv -f $(@:.o=.d) $(@:.o=.d.tmp)
	@sed 's|.*:|$@:|' < $(@:.o=.d.tmp) > $(@:.o=.d)
	@rm -f $(@:.o=.d.tmp)

clean_tiledb:
	rm -f $(TILEDB_OBJ_DIR)/* $(TILEDB_BIN_DIR)/* 

# --- Linking --- #

$(TILEDB_BIN_DIR)/tiledb: $(TILEDB_OBJ) $(CORE_OBJ)
	@mkdir -p $(TILEDB_BIN_DIR)
	$(CXX) -fopenmp -o $@ $^
 
######
# LA #
######

# --- Compilation and dependency genration --- #

-include $(LA_OBJ:.o=.d)

$(LA_OBJ_DIR)/%.o: $(LA_SRC_DIR)/%.cc
	@test -d $(LA_OBJ_DIR) || mkdir -p $(LA_OBJ_DIR)
	$(CXX) $(LA_INCLUDE_PATHS)  $(CORE_INCLUDE_PATHS) -c $< -o $@
	@$(CXX) -MM $(CORE_INCLUDE_PATHS) $(LA_INCLUDE_PATHS) $< > $(@:.o=.d)
	@mv -f $(@:.o=.d) $(@:.o=.d.tmp)
	@sed 's|.*:|$@:|' < $(@:.o=.d.tmp) > $(@:.o=.d)
	@rm -f $(@:.o=.d.tmp)

clean_la:
	rm -f $(LA_OBJ_DIR)/* $(LA_BIN_DIR)/* 

# --- Linking --- #

$(LA_BIN_DIR)/example_transpose: $(LA_OBJ) $(CORE_OBJ)
	@mkdir -p $(LA_BIN_DIR)
	$(CXX) -fopenmp -o $@ $^

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
	@echo INPUT = $(DOC_DIR)/mainpage.dox $(CORE_INCLUDE) > doxyfile.inc
	@echo FILE_PATTERNS =  *.h >> doxyfile.inc
	doxygen Doxyfile.mk

# LIB_PATHS = /usr/local/lib/libspatialindex.so
# LIBS = -lpqxx -lpthread
