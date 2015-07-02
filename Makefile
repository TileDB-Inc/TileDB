##########
# Macros #
##########

# --- Compilers --- #

# C++ compiler
# CXX = g++ 

# MPI compiler for C++
CXX = mpicxx -std=c++11 -fmax-errors=5 

# --- Directories --- #
# Directories for the core code of TileDB
CORE_INCLUDE_DIR = core/include
CORE_INCLUDE_SUBDIRS = $(wildcard core/include/*)
CORE_SRC_DIR = core/src
CORE_SRC_SUBDIRS = $(wildcard core/src/*)
CORE_OBJ_DIR = core/obj
CORE_BIN_DIR = core/bin

# Directories for the command-line-based frontend of TileDB
TILEDB_CMD_INCLUDE_DIR = tiledb_cmd/include
TILEDB_CMD_SRC_DIR = tiledb_cmd/src
TILEDB_CMD_OBJ_DIR = tiledb_cmd/obj
TILEDB_CMD_BIN_DIR = tiledb_cmd/bin

# Directories of Google Test
GTEST_DIR = gtest
GTEST_INCLUDE_DIR = gtest/include
GTEST_SRC_DIR = gtest/src
GTEST_OBJ_DIR = gtest/obj
GTEST_BIN_DIR = gtest/bin

# Directories for TileDB tests
TEST_SRC_DIR = test/src
TEST_OBJ_DIR = test/obj
TEST_BIN_DIR = test/bin

# Directories for Linear Algebra applications 
LA_INCLUDE_DIR = la/include
LA_SRC_DIR = la/src
LA_OBJ_DIR = la/obj
LA_BIN_DIR = la/bin

# Directory for Doxygen documentation
DOXYGEN_DIR = doxygen

# Manpages directories
MANPAGES_MAN_DIR = manpages/man
MANPAGES_HTML_DIR = manpages/html

# Directories for the MPI files - not necessary if mpicxx used.
MPI_INCLUDE_DIR := .
MPI_LIB_DIR := .

# Directories for the OpenMP files
OPENMP_INCLUDE_DIR = .
OPENMP_LIB_DIR = .

# --- Paths --- #
CORE_INCLUDE_PATHS = $(addprefix -I, $(CORE_INCLUDE_SUBDIRS))
TILEDB_CMD_INCLUDE_PATHS = -I$(TILEDB_CMD_INCLUDE_DIR)
LA_INCLUDE_PATHS = -I$(LA_INCLUDE_DIR)
MPI_INCLUDE_PATHS = -I$(MPI_INCLUDE_DIR)
MPI_LIB_PATHS = -L$(MPI_LIB_DIR)
OPENMP_INCLUDE_PATHS = -L$(OPENMP_INCLUDE_DIR)
OPENMP_LIB_PATHS = -L$(OPENMP_LIB_DIR)

# --- Libs --- #
MPI_LIB = -lmpi
OPENMP_LIB = -fopenmp

# --- Files --- #

# Files of the TileDB core
CORE_INCLUDE := $(foreach D,$(CORE_INCLUDE_SUBDIRS),$D/*.h) 
CORE_SRC := $(wildcard $(foreach D,$(CORE_SRC_SUBDIRS),$D/*.cc))
CORE_OBJ := $(patsubst $(CORE_SRC_DIR)/%.cc, $(CORE_OBJ_DIR)/%.o, $(CORE_SRC))

# Files of the TileDB command-line-based frontend
TILEDB_CMD_INCLUDE := $(wildcard $(TILEDB_CMD_INCLUDE_DIR)/*.h)
TILEDB_CMD_SRC := $(wildcard $(TILEDB_CMD_SRC_DIR)/*.cc)
TILEDB_CMD_OBJ := $(patsubst $(TILEDB_CMD_SRC_DIR)/%.cc,\
                             $(TILEDB_CMD_OBJ_DIR)/%.o, $(TILEDB_CMD_SRC))
TILEDB_CMD_BIN := $(patsubst $(TILEDB_CMD_SRC_DIR)/%.cc,\
                             $(TILEDB_CMD_BIN_DIR)/%, $(TILEDB_CMD_SRC)) 

# Files of the Google Test
GTEST_INCLUDE := $(wildcard $(GTEST_INCLUDE_DIR)/*.h)
GTEST_OBJ := $(patsubst $(GTEST_SRC_DIR)/%.cc, $(GTEST_OBJ_DIR)/%.o,\
                        $(GTEST_SRC))

# Files of the TileDB tests
TEST_SRC := $(wildcard $(TEST_SRC_DIR)/*.cc)
TEST_OBJ := $(patsubst $(TEST_SRC_DIR)/%.cc, $(TEST_OBJ_DIR)/%.o, $(TEST_SRC))

# Files of the Linear Algebra applications
LA_SRC := $(wildcard $(LA_SRC_DIR)/*.cc)
LA_OBJ := $(patsubst $(LA_SRC_DIR)/%.cc, $(LA_OBJ_DIR)/%.o, $(LA_SRC))
LA_BIN := $(patsubst $(LA_SRC_DIR)/%.cc, $(LA_BIN_DIR)/%, $(LA_SRC))

# Files for the HTML version of the Manpages
MANPAGES_MAN := $(wildcard $(MANPAGES_MAN_DIR)/*)
MANPAGES_HTML := $(patsubst $(MANPAGES_MAN_DIR)/%,\
                            $(MANPAGES_HTML_DIR)/%.html, $(MANPAGES_MAN)) 

###################
# General Targets #
###################

.PHONY: core example gtest test doc clean_core clean_gtest \
        clean_test clean_tiledb_cmd clean_la clean

all: core tiledb_cmd la gtest test 

core: $(CORE_OBJ) 

tiledb_cmd: core $(TILEDB_CMD_BIN)

la: core $(LA_OBJ) $(LA_BIN_DIR)/example_transpose

doc: doxyfile.inc $(MANPAGES_HTML) 

gtest: $(GTEST_OBJ_DIR)/gtest-all.o

test: $(TEST_OBJ)

clean: clean_core clean_tiledb_cmd clean_la clean_gtest \
       clean_test clean_doc 

########
# Core #
########

# --- Compilation and dependency genration --- #

-include $(CORE_OBJ:%.o=%.d)

$(CORE_OBJ_DIR)/%.o: $(CORE_SRC_DIR)/%.cc
	@mkdir -p $(dir $@) 
	@echo "Compiling $<"
	@$(CXX) $(CORE_INCLUDE_PATHS) $(OPENMP_INCLUDE_PATHS) \
                $(MPI_INCLUDE_PATHS) -c $< -o $@
	@$(CXX) -MM $(CORE_INCLUDE_PATHS) $< > $(@:.o=.d)
	@mv -f $(@:.o=.d) $(@:.o=.d.tmp)
	@sed 's|.*:|$@:|' < $(@:.o=.d.tmp) > $(@:.o=.d)
	@rm -f $(@:.o=.d.tmp)

# --- Cleaning --- #

clean_core: 
	@echo 'Cleaning core'
	@rm -rf $(CORE_OBJ_DIR)/* $(CORE_BIN_DIR)/* 

##############
# TileDB_cmd #
##############

# --- Compilation and dependency genration --- #

-include $(TILEDB_CMD_OBJ:.o=.d)

$(TILEDB_CMD_OBJ_DIR)/%.o: $(TILEDB_CMD_SRC_DIR)/%.cc
	@mkdir -p $(TILEDB_CMD_OBJ_DIR)
	@echo "Compiling $<"
	@$(CXX) $(TILEDB_CMD_INCLUDE_PATHS) $(CORE_INCLUDE_PATHS) -c $< -o $@
	@$(CXX) -MM $(TILEDB_CMD_INCLUDE_PATHS) \
                    $(CORE_INCLUDE_PATHS) $< > $(@:.o=.d)
	@mv -f $(@:.o=.d) $(@:.o=.d.tmp)
	@sed 's|.*:|$@:|' < $(@:.o=.d.tmp) > $(@:.o=.d)
	@rm -f $(@:.o=.d.tmp)

# --- Linking --- #

$(TILEDB_CMD_BIN_DIR)/%: $(TILEDB_CMD_OBJ_DIR)/%.o $(CORE_OBJ)
	@mkdir -p $(TILEDB_CMD_BIN_DIR)
	@echo "Creating $@"
	@$(CXX) $(OPENMP_LIB_PATHS) $(OPENMP_LIB) $(MPI_LIB_PATHS) $(MPI_LIB) \
                -o $@ $^

# --- Cleaning --- #

clean_tiledb_cmd:
	@echo 'Cleaning tiledb_cmd'
	@rm -f $(TILEDB_CMD_OBJ_DIR)/* $(TILEDB_CMD_BIN_DIR)/* 
 
######
# LA #
######

# --- Compilation and dependency genration --- #

-include $(LA_OBJ:.o=.d)

$(LA_OBJ_DIR)/%.o: $(LA_SRC_DIR)/%.cc
	@test -d $(LA_OBJ_DIR) || mkdir -p $(LA_OBJ_DIR)
	@echo "Compiling $<"
	@$(CXX) $(LA_INCLUDE_PATHS) $(CORE_INCLUDE_PATHS) -c $< -o $@
	@$(CXX) -MM $(CORE_INCLUDE_PATHS) $(LA_INCLUDE_PATHS) $< > $(@:.o=.d)
	@mv -f $(@:.o=.d) $(@:.o=.d.tmp)
	@sed 's|.*:|$@:|' < $(@:.o=.d.tmp) > $(@:.o=.d)
	@rm -f $(@:.o=.d.tmp)

# --- Linking --- #

$(LA_BIN_DIR)/example_transpose: $(LA_OBJ) $(CORE_OBJ)
	@mkdir -p $(LA_BIN_DIR)
	@echo "Creating example_transpose"
	@$(CXX) $(OPENMP_LIB_PATHS) $(OPENMP_LIB) $(MPI_LIB_PATHS) $(MPI_LIB) \
               -o $@ $^

# --- Cleaning --- #

clean_la:
	@echo 'Cleaning la'
	@rm -f $(LA_OBJ_DIR)/* $(LA_BIN_DIR)/* 

###############
# Google Test #
###############

# --- Compilation --- #

$(GTEST_OBJ_DIR)/gtest-all.o: gtest/src/gtest-all.cc \
                              $(wildcard gtest/include/gtest/*.h)
	@test -d $(GTEST_OBJ_DIR) || mkdir -p $(GTEST_OBJ_DIR)
	@echo "Compiling $<"
	@$(CXX) -isystem $(GTEST_INCLUDE_DIR) -I$(GTEST_DIR) \
                -pthread -c $< -o $@

# --- Cleaning --- #

clean_gtest:
	@echo "Cleaning gtest"
	@rm -f $(GTEST_OBJ_DIR)/* $(GTEST_BIN_DIR)/* 

################
# TileDB Tests #
################

# Coming up soon...

# --- Cleaning --- #

clean_test:
	@echo "Cleaning test"
	@rm -f $(TEST_OBJ_DIR)/* $(TEST_BIN_DIR)/* 

################################
# Documentation (with Doxygen) #
################################

doxyfile.inc: $(CORE_INCLUDE) $(TILEDB_CMD_INCLUDE) $(LA_INCLUDE)
	@echo 'Creating Doxygen documentation'
	@echo INPUT = $(DOXYGEN_DIR)/mainpage.dox $(CORE_INCLUDE) \
                      $(TILEDB_CMD_INCLUDE) $(LA_INCLUDE) > doxyfile.inc
	@echo FILE_PATTERNS = *.h >> doxyfile.inc
	@doxygen Doxyfile.mk > Doxyfile.log 2>&1

echo_manpages_msg:
	@echo 'Converting Manpages to HTML'

$(MANPAGES_HTML_DIR)/%.html: $(MANPAGES_MAN_DIR)/% echo_manpages_msg  
	@man2html $< > $@

# --- Cleaning --- #

clean_doc:
	@echo "Cleaning documentation"
	@rm -f doxyfile.inc
	@rm -f $(MANPAGES_HTML)

