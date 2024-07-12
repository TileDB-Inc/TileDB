/**
 * @file unit-cppapi-query-condition-enumerations.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB Inc.
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
 * Tests the C++ API for query condition related functions.
 */

#include <limits>
#include <ostream>
#include <random>

#include "test/support/src/ast_helpers.h"
#include "test/support/tdb_catch.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"
#include "tiledb/sm/serialization/query.h"

#ifdef TILEDB_SERIALIZATION
#include <capnp/message.h>
#include <capnp/serialize.h>
#define GENERATE_SERIALIZATION() GENERATE(false, true)
#else
#define GENERATE_SERIALIZATION() false
#endif

using namespace tiledb;

/*
 * The test fixture. See the first test for a basic example of expected
 * usage.
 */

struct EnmrQCCell;
struct ResultEnmrQCCell;

using EnmrQCMatcher = std::function<bool(const EnmrQCCell& cell)>;
using EnmrQCCreator = std::function<QueryCondition(Context& ctx)>;

class CPPQueryConditionEnumerationFx {
 public:
  CPPQueryConditionEnumerationFx();
  ~CPPQueryConditionEnumerationFx();

  uint32_t run_test(
      tiledb_array_type_t type,
      bool serialize,
      EnmrQCMatcher matcher,
      EnmrQCCreator creator,
      uint32_t num_rows = 1024);

 protected:
  std::string uri_;
  Context ctx_;
  VFS vfs_;

  tiledb_array_type_t type_;
  bool serialize_;
  uint32_t num_rows_;

  // A fill value result. This is the value that a dense array query returns
  // for a non-matching result.
  std::unique_ptr<EnmrQCCell> fill_;

  // Our random source
  std::mt19937_64 rand_;

  // Enumeration helpers
  std::unordered_map<std::string, uint8_t> cell_type_values_;
  std::unordered_map<std::string, uint8_t> cycle_phase_values_;
  std::unordered_map<std::string, uint8_t> wavelength_values_;

  std::unordered_map<uint8_t, std::string> cell_type_index_;
  std::unordered_map<uint8_t, std::string> cycle_phase_index_;
  std::unordered_map<uint8_t, std::string> wavelength_index_;

  // The data in the array represented as a vector of EnmrQCCell instances.
  std::vector<EnmrQCCell> data_;

  // Private API
  void create_array(
      tiledb_array_type_t type, bool serialize, uint32_t num_rows);
  void remove_array();
  void write_array();
  uint32_t check_read(EnmrQCMatcher matcher, EnmrQCCreator creator);
  std::vector<ResultEnmrQCCell> read_array(EnmrQCCreator creator);
  std::vector<EnmrQCCell> generate_data(uint32_t num_rows);
  void create_enumeration(
      ArraySchema& schema,
      const std::string& name,
      const std::unordered_map<uint8_t, std::string>& values,
      bool ordered);
  QueryCondition serialize_deserialize_qc(QueryCondition& qc);
  void validate_query_condition(EnmrQCCreator creator);
  std::unordered_map<uint8_t, std::string> make_index(
      std::unordered_map<std::string, uint8_t> values);
};

/*
 * Test Schema
 * ===========
 *
 * row_id - A numeric integer in the range 1 - $NUM_ROWS
 * sample_name - A random string with the format [A-J]{4}[0-9]{8}
 * cell_type - An enumeration of cell types, listed below.
 * cycle_phase - A nullable enumeration of cell cycle phase, listed below.
 * wavelength - An ordered enumeration of laser wavelengths, listed below.
 * luminosity - A float value in the range [0.0, 1.0]
 *
 * Cell Type Enumeration Values:
 *
 *   For the non biologists: Endothelial cells have to do with blood vessels
 *   and epithelial has to do with skin and other membranes. Stem cells are
 *   progenitors that can become other types of cells, and neurons are cells
 *   in the brain. Muscle and bone cell types are both self documenting.
 *
 *   - endothelial
 *   - epithelial
 *   - muscle
 *   - bone
 *   - neuron
 *   - stem
 *
 * Cell Cycle Phases (These are actually real):
 *
 *   Fun fact, G1 and G2 literally stand for Gap 1 and Gap 2. M stands for the
 *   mitosis/meiosis stage (i.e., cell division), S is the synthesis phase
 *   (i.e., when a cell is replicating its DNA in preparation to divide), while
 *   G1 and G2 are basically a historical "We're not sure what's going on
 *   exactly" stages. I'm sure they know more now, but this entire anecdote is
 *   the only reason I remember the stages.
 *
 *   Also, this enumeration is ordered in this test even though it really
 *   hasn't got an order since there's no obvious first step of the cycle given
 *   that its actually the definition of a chicken and egg issue.
 *
 *   - G1
 *   - S
 *   - G2
 *   - M
 *
 * Laser Wavelengths (Also real, but no, I don't have these memorized):
 *
 * N.B., the values are "355nm" or "552nm" for example. I've labeled each
 * wavelength with their corresponding color only for reference for folks that
 * haven't memorized the electromagnetic spectrum.
 *
 * Also, a quick background on the science of fluorescent microscopy and why
 * wavelengths as an ordered enumeration is actually an interesting use case.
 * First, the basic principle of fluorescence is that an atom or molecule can
 * be excited by a photon of a certain frequency into a new state, which
 * then after some time relaxes and emits a photon of a different wavelength.
 * Anything that can do this is called a fluorophore. The important part here
 * is that the both of the excitation and relaxation photons are set at
 * specific wavelengths because physics.
 *
 * The result of all that is that you can detect fluorophores by shining
 * one color of light on it and then looking for a specific *different* color
 * of light being emitted. With that knowledge, applying it to science is just
 * a matter of tagging something of interest with a fluorophore and then
 * setting up various light sources and wavelength filters and voila, you get
 * a useful measurable signal.
 *
 * So back to lasers, given that we have specific wavelengths that are chosen
 * based on what fluorophore we're using, we wouldn't want this to just be a
 * integer. Allowing raw integral values means that there's a possibility we
 * end up with data that's not one of our lasers due to data entry
 * errors and so on. However, they're quite comparable as obviously the
 * enumerated values are numeric in nature.
 *
 *   - 355nm (ultra violet)
 *   - 405nm (blue)
 *   - 488nm (violet)
 *   - 532nm (green)
 *   - 552nm (greener?)
 *   - 561nm (green-yellow)
 *   - 640nm (red)
 */

struct EnmrQCCell {
  EnmrQCCell();

  uint32_t row_id;
  std::string sample_name;
  std::string cell_type;
  std::string cycle_phase;
  bool cycle_phase_valid;
  std::string wavelength;
  float luminosity;
};

// Used by test internals
struct ResultEnmrQCCell : public EnmrQCCell {
  ResultEnmrQCCell();

  // We're purposefully avoiding a copy constructor so that the single case
  // we need to copy a fill value is made obvious.
  void copy_fill(const std::unique_ptr<EnmrQCCell>& rhs);

  bool operator==(const EnmrQCCell& rhs);
  bool valid;
};

/*
 * Test case definitions start here.
 */

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "Basic Tests",
    "[query-condition][enumeration][logic]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();
  auto threshold = num_rows_ / 2;
  auto matcher = [=](const EnmrQCCell& cell) {
    return cell.row_id < threshold;
  };
  auto creator = [=](Context& ctx) {
    return QueryCondition::create(ctx, "row_id", threshold, TILEDB_LT);
  };

  run_test(type, serialize, matcher, creator);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "Simple Enumeration Equality",
    "[query-condition][enumeration][logic]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();
  auto matcher = [](const EnmrQCCell& cell) {
    return cell.cell_type == "bone";
  };
  auto creator = [](Context& ctx) {
    return QueryCondition::create(
        ctx, "cell_type", std::string("bone"), TILEDB_EQ);
  };

  run_test(type, serialize, matcher, creator);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "Simple Enumeration Non-Equality",
    "[query-condition][enumeration][logic]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();
  auto matcher = [](const EnmrQCCell& cell) {
    return cell.cell_type != "bone";
  };
  auto creator = [](Context& ctx) {
    return QueryCondition::create(
        ctx, "cell_type", std::string("bone"), TILEDB_NE);
  };

  run_test(type, serialize, matcher, creator);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "Simple Enumeration Inequality",
    "[query-condition][enumeration][logic]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();
  auto matcher = [](const EnmrQCCell& cell) {
    return cell.wavelength <= "532nm";
  };
  auto creator = [](Context& ctx) {
    return QueryCondition::create(
        ctx, "wavelength", std::string("532nm"), TILEDB_LE);
  };

  run_test(type, serialize, matcher, creator);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "Simple Enumeration Equality to Invalid Value",
    "[query-condition][enumeration][logic]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();
  auto matcher = [](const EnmrQCCell& cell) {
    return cell.cell_type == "fruit";
  };
  auto creator = [](Context& ctx) {
    return QueryCondition::create(
        ctx, "cell_type", std::string("fruit"), TILEDB_EQ);
  };

  // Assert that == invalid enumeration value matches nothing.
  auto matched = run_test(type, serialize, matcher, creator);
  REQUIRE(matched == 0);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "Simple Enumeration Non-Equality to Invalid Value",
    "[query-condition][enumeration][logic]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();
  auto matcher = [](const EnmrQCCell& cell) {
    return cell.cell_type != "fruit";
  };
  auto creator = [](Context& ctx) {
    return QueryCondition::create(
        ctx, "cell_type", std::string("fruit"), TILEDB_NE);
  };

  // Assert that != invalid value matches everything.
  auto matched = run_test(type, serialize, matcher, creator);
  REQUIRE(matched == num_rows_);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "Enumeration Equality to Negated Invalid Value",
    "[query-condition][enumeration][logic]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();
  auto matcher = [](const EnmrQCCell& cell) {
    return cell.cell_type == "fruit";
  };
  auto creator = [](Context& ctx) {
    auto qc = QueryCondition::create(
        ctx, "cell_type", std::string("fruit"), TILEDB_NE);
    return qc.negate();
  };

  // Assert that (not !=) invalid value matches nothing.
  auto matched = run_test(type, serialize, matcher, creator);
  REQUIRE(matched == 0);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "Enumeration Non-Equality to Negated Invalid Value",
    "[query-condition][enumeration][logic]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();
  auto matcher = [](const EnmrQCCell& cell) {
    return cell.cell_type != "fruit";
  };
  auto creator = [](Context& ctx) {
    auto qc = QueryCondition::create(
        ctx, "cell_type", std::string("fruit"), TILEDB_EQ);
    return qc.negate();
  };

  // Assert that (not ==) invalid value matches everything
  auto matched = run_test(type, serialize, matcher, creator);
  REQUIRE(matched == num_rows_);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "Enumeration Inequality with Invalid Value",
    "[query-condition][enumeration][logic]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();
  auto matcher = [](const EnmrQCCell&) { return false; };
  auto creator = [](Context& ctx) {
    return QueryCondition::create(
        ctx, "wavelength", std::string("6000nm"), TILEDB_LE);
  };

  // Assert that (<=) invalid value matches nothing.
  auto matched = run_test(type, serialize, matcher, creator);
  REQUIRE(matched == 0);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "Enumeration Inequality with Negated Invalid Value",
    "[query-condition][enumeration][logic]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();
  auto matcher = [](const EnmrQCCell&) { return false; };
  auto creator = [](Context& ctx) {
    auto qc = QueryCondition::create(
        ctx, "wavelength", std::string("6000nm"), TILEDB_LE);
    return qc.negate();
  };

  // Assert that (not <=) invalid value matches nothing.
  auto matched = run_test(type, serialize, matcher, creator);
  REQUIRE(matched == 0);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "Enumeration IN Set with Invalid Member",
    "[query-condition][enumeration][logic]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();
  auto matcher = [](const EnmrQCCell& cell) {
    return cell.cell_type == "bone" || cell.cell_type == "stem";
  };
  auto creator = [](Context& ctx) {
    std::vector<std::string> values = {"bone", "stem", "fish"};
    return QueryConditionExperimental::create(
        ctx, "cell_type", values, TILEDB_IN);
  };

  run_test(type, serialize, matcher, creator);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "Enumeration NOT_IN Set with Invalid Member",
    "[query-condition][enumeration][logic]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();
  auto matcher = [](const EnmrQCCell& cell) {
    return cell.cell_type != "bone" && cell.cell_type != "stem";
  };
  auto creator = [](Context& ctx) {
    std::vector<std::string> values = {"bone", "stem", "fish"};
    return QueryConditionExperimental::create(
        ctx, "cell_type", values, TILEDB_NOT_IN);
  };

  run_test(type, serialize, matcher, creator);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "Enumeration IN Set with Negated Invalid Member",
    "[query-condition][enumeration][logic]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();
  auto matcher = [](const EnmrQCCell& cell) {
    return cell.cell_type == "bone" || cell.cell_type == "stem";
  };
  auto creator = [](Context& ctx) {
    std::vector<std::string> values = {"bone", "stem", "fish"};
    auto qc = QueryConditionExperimental::create(
        ctx, "cell_type", values, TILEDB_NOT_IN);
    return qc.negate();
  };

  run_test(type, serialize, matcher, creator);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "Enumeration NOT IN Set with Negated Invalid Member",
    "[query-condition][enumeration][logic]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();
  auto matcher = [](const EnmrQCCell& cell) {
    return cell.cell_type != "bone" && cell.cell_type != "stem";
  };
  auto creator = [](Context& ctx) {
    std::vector<std::string> values = {"bone", "stem", "fish"};
    auto qc =
        QueryConditionExperimental::create(ctx, "cell_type", values, TILEDB_IN);
    return qc.negate();
  };

  run_test(type, serialize, matcher, creator);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "RowID inequality AND Enumeration IN Set with Invalid Member",
    "[query-condition][enumeration][logic]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();
  auto matcher = [](const EnmrQCCell& cell) {
    auto r1 = cell.row_id < 512;
    auto r2 = cell.cell_type == "bone" || cell.cell_type == "stem";
    return r1 && r2;
  };
  auto creator = [](Context& ctx) {
    auto qc1 = QueryCondition::create(ctx, "row_id", 512, TILEDB_LT);
    std::vector<std::string> values = {"bone", "stem", "fish"};
    auto qc2 =
        QueryConditionExperimental::create(ctx, "cell_type", values, TILEDB_IN);
    return qc1.combine(qc2, TILEDB_AND);
  };

  run_test(type, serialize, matcher, creator);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "RowID inequality OR Enumeration NOT_IN Set with Invalid Member",
    "[query-condition][enumeration][logic]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();
  auto matcher = [](const EnmrQCCell& cell) {
    auto r1 = cell.row_id < 512;
    auto r2 = cell.cell_type != "bone" && cell.cell_type != "stem";
    return r1 || r2;
  };
  auto creator = [](Context& ctx) {
    auto qc1 = QueryCondition::create(ctx, "row_id", 512, TILEDB_LT);
    std::vector<std::string> values = {"bone", "stem", "fish"};
    auto qc2 = QueryConditionExperimental::create(
        ctx, "cell_type", values, TILEDB_NOT_IN);
    return qc1.combine(qc2, TILEDB_OR);
  };

  run_test(type, serialize, matcher, creator);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "Check error on negation of TILEDB_ALWAYS_TRUE after rewrite.",
    "[query-condition][enumeration][logic][rewrite-error]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();

  create_array(type, serialize, 1024);

  Array array(ctx_, uri_, TILEDB_READ);
  auto core_array = array.ptr().get()->array_;
  core_array->load_all_enumerations();

  auto qc =
      QueryCondition::create(ctx_, "cell_type", std::string("fish"), TILEDB_NE);
  auto core_qc = qc.ptr().get()->query_condition_;
  core_qc->rewrite_enumeration_conditions(core_array->array_schema_latest());

  auto matcher = Catch::Matchers::ContainsSubstring(
      "Invalid negation of rewritten query.");
  REQUIRE_THROWS_WITH(qc.negate(), matcher);
}

TEST_CASE_METHOD(
    CPPQueryConditionEnumerationFx,
    "Check error on negation of TILEDB_ALWAYS_FALSE after rewrite.",
    "[query-condition][enumeration][logic][rewrite-error]") {
  auto type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  auto serialize = GENERATE_SERIALIZATION();

  create_array(type, serialize, 1024);

  Array array(ctx_, uri_, TILEDB_READ);
  auto core_array = array.ptr().get()->array_;
  core_array->load_all_enumerations();

  auto qc =
      QueryCondition::create(ctx_, "cell_type", std::string("fish"), TILEDB_EQ);
  auto core_qc = qc.ptr().get()->query_condition_;
  core_qc->rewrite_enumeration_conditions(core_array->array_schema_latest());

  auto matcher = Catch::Matchers::ContainsSubstring(
      "Invalid negation of rewritten query.");
  REQUIRE_THROWS_WITH(qc.negate(), matcher);
}

TEST_CASE(
    "Check error on creating a TILEDB_ALWAYS_TRUE QueryCondition",
    "[query-condition][enumeration][logic][op-error]") {
  Context ctx;
  // TILEDB_ALWAYS_TRUE is not an exposed symbol so we even have to force
  // the issue by knowing the internal value and casting it.
  auto op = static_cast<tiledb_query_condition_op_t>(253);
  auto matcher = Catch::Matchers::ContainsSubstring(
      "Invalid use of internal operation: ALWAYS_TRUE");
  REQUIRE_THROWS_WITH(QueryCondition::create(ctx, "foo", 0, op), matcher);
}

TEST_CASE(
    "Check error on creating a TILEDB_ALWAYS_FALSE QueryCondition",
    "[query-condition][enumeration][logic][op-error]") {
  Context ctx;
  // TILEDB_ALWAYS_FALSE is not an exposed symbol so we even have to force
  // the issue by knowing the internal value and casting it.
  auto op = static_cast<tiledb_query_condition_op_t>(254);
  auto matcher = Catch::Matchers::ContainsSubstring(
      "Invalid use of internal operation: ALWAYS_FALSE");
  REQUIRE_THROWS_WITH(QueryCondition::create(ctx, "foo", 0, op), matcher);
}

/*
 * All code below here is test support implementation.
 */

EnmrQCCell::EnmrQCCell()
    : row_id(0)
    , sample_name("Uninitialied Data Cell")
    , cell_type("Uninitialised Data Cell")
    , cycle_phase("Uninitialized Data Cell")
    , wavelength("Uninitialized Data Cell")
    , luminosity(3.14159f) {
}

ResultEnmrQCCell::ResultEnmrQCCell() {
  row_id = std::numeric_limits<uint32_t>::max();
  sample_name = "Uninitialized Result Cell";
  cell_type = "Uninitialized Result Cell";
  cycle_phase = "Uninitialized Result Cell";
  wavelength = "Uninitialized Result Cell";
  luminosity = 1.618f;
  valid = false;
}

void ResultEnmrQCCell::copy_fill(const std::unique_ptr<EnmrQCCell>& rhs) {
  row_id = rhs->row_id;
  sample_name = rhs->sample_name;
  cell_type = rhs->cell_type;
  cycle_phase = rhs->cycle_phase;
  wavelength = rhs->wavelength;
  luminosity = rhs->luminosity;
  valid = true;
}

bool ResultEnmrQCCell::operator==(const EnmrQCCell& rhs) {
  if (row_id != rhs.row_id) {
    return false;
  }

  if (sample_name != rhs.sample_name) {
    return false;
  }

  if (cell_type != rhs.cell_type) {
    return false;
  }

  if (cycle_phase != rhs.cycle_phase) {
    return false;
  }

  if (wavelength != rhs.wavelength) {
    return false;
  }

  if (luminosity != rhs.luminosity) {
    return false;
  }

  return true;
}

std::ostream& operator<<(std::ostream& os, const EnmrQCCell& cell) {
  return os << "EnmrQCCell{"
            << "row_id: " << cell.row_id << ", "
            << "sample_name: '" << cell.sample_name << "', "
            << "cell_type: '" << cell.cell_type << "', "
            << "cycle_phase: '" << cell.cycle_phase << "', "
            << "cycle_phase_valid: " << (cell.cycle_phase_valid ? "yes" : "no")
            << ", "
            << "wavelength: '" << cell.wavelength << "', "
            << "luminosity: " << cell.luminosity << "}";
}

CPPQueryConditionEnumerationFx::CPPQueryConditionEnumerationFx()
    : uri_("query_condition_enumeration_array")
    , vfs_(ctx_) {
  remove_array();

  // This is used for asserting the dense-non-match case.
  fill_ = std::make_unique<EnmrQCCell>();
  fill_->sample_name = "";
  fill_->cell_type = "";
  fill_->cycle_phase = "";
  fill_->cycle_phase_valid = false;
  fill_->wavelength = "";
  fill_->luminosity = std::numeric_limits<float>::min();

  std::random_device rdev;
  rand_.seed(rdev());

  cell_type_values_ = {
      {"bone", 0},
      {"endothelial", 1},
      {"epithelial", 2},
      {"muscle", 3},
      {"neuron", 4},
      {"stem", 5}};

  cycle_phase_values_ = {{"G1", 0}, {"S", 1}, {"G2", 2}, {"M", 3}};

  wavelength_values_ = {
      {"355nm", 0},
      {"405nm", 1},
      {"488nm", 2},
      {"532nm", 3},
      {"552nm", 4},
      {"561nm", 5},
      {"640nm", 6}};

  cell_type_index_ = make_index(cell_type_values_);
  cycle_phase_index_ = make_index(cycle_phase_values_);
  wavelength_index_ = make_index(wavelength_values_);
}

CPPQueryConditionEnumerationFx::~CPPQueryConditionEnumerationFx() {
  remove_array();
}

uint32_t CPPQueryConditionEnumerationFx::run_test(
    tiledb_array_type_t type,
    bool serialize,
    EnmrQCMatcher matcher,
    EnmrQCCreator creator,
    uint32_t num_rows) {
  create_array(type, serialize, num_rows);
  return check_read(matcher, creator);
}

void CPPQueryConditionEnumerationFx::create_array(
    tiledb_array_type_t type, bool serialize, uint32_t num_rows) {
  type_ = type;
  serialize_ = serialize;
  num_rows_ = num_rows;
  data_ = generate_data(num_rows_);

  // Create our array schema
  ArraySchema schema(ctx_, type_);

  if (type_ == TILEDB_SPARSE) {
    schema.set_capacity(num_rows_);
  }

  // Create a single dimension row_id as uint32_t
  auto dim = Dimension::create<uint32_t>(ctx_, "row_id", {{1, num_rows_}});
  auto dom = Domain(ctx_);
  dom.add_dimension(dim);
  schema.set_domain(dom);

  // Create our enumerations
  create_enumeration(schema, "cell_types", cell_type_index_, false);
  create_enumeration(schema, "cycle_phases", cycle_phase_index_, true);
  create_enumeration(schema, "wavelengths", wavelength_index_, true);

  // Create our attributes
  auto sample_name = Attribute::create<std::string>(ctx_, "sample_name");

  auto cell_type = Attribute::create<uint8_t>(ctx_, "cell_type");
  AttributeExperimental::set_enumeration_name(ctx_, cell_type, "cell_types");

  auto cell_phase = Attribute::create<uint8_t>(ctx_, "cycle_phase");
  AttributeExperimental::set_enumeration_name(ctx_, cell_phase, "cycle_phases");
  cell_phase.set_nullable(true);

  auto wavelength = Attribute::create<uint8_t>(ctx_, "wavelength");
  AttributeExperimental::set_enumeration_name(ctx_, wavelength, "wavelengths");

  auto luminosity = Attribute::create<float>(ctx_, "luminosity");

  schema.add_attributes(
      sample_name, cell_type, cell_phase, wavelength, luminosity);

  // Create and write the array.
  Array::create(uri_, schema);
  write_array();
}

void CPPQueryConditionEnumerationFx::write_array() {
  Array array(ctx_, uri_, TILEDB_WRITE);
  Query query(ctx_, array);

  std::vector<uint32_t> row_ids(num_rows_);
  std::iota(row_ids.begin(), row_ids.end(), 1);

  if (type_ == TILEDB_DENSE) {
    Subarray subarray(ctx_, array);
    subarray.add_range<uint32_t>(0, 1, num_rows_);
    query.set_subarray(subarray);
  } else {
    query.set_data_buffer("row_id", row_ids);
  }

  // Generate our write buffers
  std::vector<char> names(num_rows_ * strlen("AAAA00000000"));
  std::vector<uint64_t> name_offsets(num_rows_);
  std::vector<uint8_t> cell_types(num_rows_);
  std::vector<uint8_t> cycle_phases(num_rows_);
  std::vector<uint8_t> cycle_phases_validity(num_rows_);
  std::vector<uint8_t> wavelengths(num_rows_);
  std::vector<float> luminosity(num_rows_);

  uint64_t name_offset = 0;
  for (size_t i = 0; i < num_rows_; i++) {
    auto& cell = data_[i];

    std::memcpy(
        names.data() + name_offset,
        cell.sample_name.data(),
        cell.sample_name.size());
    name_offsets[i] = name_offset;
    name_offset += cell.sample_name.size();

    cell_types[i] = cell_type_values_.at(cell.cell_type);
    if (cell.cycle_phase_valid) {
      cycle_phases[i] = cycle_phase_values_.at(cell.cycle_phase);
    } else {
      cycle_phases[i] = 254;
    }
    cycle_phases_validity[i] = cell.cycle_phase_valid ? 1 : 0;
    wavelengths[i] = wavelength_values_.at(cell.wavelength);
    luminosity[i] = cell.luminosity;
  }

  // Attach the buffers to our write query
  query.set_data_buffer("sample_name", names)
      .set_offsets_buffer("sample_name", name_offsets)
      .set_data_buffer("cell_type", cell_types)
      .set_data_buffer("cycle_phase", cycle_phases)
      .set_validity_buffer("cycle_phase", cycle_phases_validity)
      .set_data_buffer("wavelength", wavelengths)
      .set_data_buffer("luminosity", luminosity);

  CHECK_NOTHROW(query.submit() == Query::Status::COMPLETE);
  query.finalize();
  array.close();
}

uint32_t CPPQueryConditionEnumerationFx::check_read(
    EnmrQCMatcher matcher, EnmrQCCreator creator) {
  validate_query_condition(creator);

  // Calculate the number of matches to expect.
  uint32_t should_match = 0;
  for (auto& cell : data_) {
    if (matcher(cell)) {
      should_match += 1;
    }
  }

  auto results = read_array(creator);

  uint32_t num_matched = 0;

  for (size_t i = 0; i < num_rows_; i++) {
    if (type_ == TILEDB_DENSE) {
      // Dense reads always return a value where non-matching cells are just
      // the fill values for all attributes.
      if (matcher(data_[i])) {
        REQUIRE(results[i] == data_[i]);
        num_matched += 1;
      } else {
        REQUIRE(results[i] == *fill_.get());
      }
      // Just an internal test assertion that all dense values are valid.
      REQUIRE(results[i].valid);
    } else {
      // Sparse queries only return cells that match. We mark this with whether
      // the ResultEnmrQCCell has its valid flag set or not.
      if (matcher(data_[i])) {
        REQUIRE(results[i] == data_[i]);
        num_matched += 1;
      } else {
        REQUIRE(results[i].valid == false);
      }
    }
  }

  REQUIRE(num_matched == should_match);

  return num_matched;
}

std::vector<ResultEnmrQCCell> CPPQueryConditionEnumerationFx::read_array(
    EnmrQCCreator creator) {
  Array array(ctx_, uri_, TILEDB_READ);
  Query query(ctx_, array);

  if (type_ == TILEDB_DENSE) {
    Subarray subarray(ctx_, array);
    subarray.add_range<uint32_t>(0, 1, num_rows_);
    query.set_subarray(subarray);
  } else {
    query.set_layout(TILEDB_GLOBAL_ORDER);
  }

  std::vector<uint32_t> row_ids(num_rows_);
  std::vector<char> sample_names(num_rows_ * 2 * strlen("AAAA00000000"));
  std::vector<uint64_t> sample_name_offsets(num_rows_);
  std::vector<uint8_t> cell_types(num_rows_);
  std::vector<uint8_t> cycle_phases(num_rows_);
  std::vector<uint8_t> cycle_phases_validity(num_rows_);
  std::vector<uint8_t> wavelengths(num_rows_);
  std::vector<float> luminosities(num_rows_);

  auto qc = creator(ctx_);
  if (serialize_) {
    qc = serialize_deserialize_qc(qc);
  }

  query.set_condition(qc)
      .set_data_buffer("row_id", row_ids)
      .set_data_buffer("sample_name", sample_names)
      .set_offsets_buffer("sample_name", sample_name_offsets)
      .set_data_buffer("cell_type", cell_types)
      .set_data_buffer("cycle_phase", cycle_phases)
      .set_validity_buffer("cycle_phase", cycle_phases_validity)
      .set_data_buffer("wavelength", wavelengths)
      .set_data_buffer("luminosity", luminosities);

  REQUIRE(query.submit() == Query::Status::COMPLETE);

  auto table = query.result_buffer_elements();

  row_ids.resize(table["row_id"].second);
  sample_name_offsets.resize(table["sample_name"].first);
  cell_types.resize(table["cell_type"].second);
  cycle_phases.resize(table["cycle_phase"].second);
  cycle_phases_validity.resize(table["cycle_phase"].second);
  wavelengths.resize(table["wavelength"].second);
  luminosities.resize(table["luminosity"].second);

  // Create our result cell instances
  //
  // Remember here that the default constructed instances are in the
  // test-uninitialized state.
  //
  // The second thing to remember, is that this for loop is has two slightly
  // different behaviors between dense and sparse queries. For spares, we can
  // iterate over 0, 1, or up to num_rows_ matches. For dense, it always
  // iterates of num_rows_ entries because non-matches are returned as fill
  // values.
  std::vector<ResultEnmrQCCell> ret(num_rows_);
  for (size_t i = 0; i < row_ids.size(); i++) {
    auto row_id = row_ids[i];

    // There are basically three states a result can be in. The first obvious
    // case is when its a match and it should equal the cell in data_. The
    // second obvious case is when its a non-match which should never ever match
    // anything in data_. The third case that makes things weird is a non-match
    // on a dense array which returns fill values.
    //
    // The logic below is dealing with each of those cases. Currently it relies
    // on a bit of a hack. This is using an implementation detail to detect the
    // difference between the non-match and dense-fill-values cases. We can do
    // this because when we write null cycle phase values, we set the cycle
    // phase enumeration value to 254. We do that on purpose to distinguish
    // these cases. Core will return what we write regardless of the null-ness
    // so we're abusing that for testing here.
    //
    // So lets dive in:
    //
    // If cycle_phase is 255, this is the dense-non-match case so the cell is
    // a copy of the random dense_non_match_ instance..
    if (cycle_phases[i] == std::numeric_limits<uint8_t>::max()) {
      ret[i].copy_fill(fill_);
      continue;
    }

    // At this point the dense-non-match case is handled. So now all we have
    // to worry about is match vs non-match cases. The following logic could
    // easily seem redundant when we could just check one attribute and return
    // a default constructed ResultEnmrQCCell or a copy of data_[i].
    //
    // However, we can't rely on the compiler defaults for asserting match
    // semantics here because of the nullptr ternary logic involved in the
    // cycle_phase case. That's a fancy way of saying (x < null) and
    // (x > null) are both false and the compiler can't figure that out for us.

    // A subtle dense vs sparse issue here. We're setting result[i].valid to
    // true because the default constructed value is false. This gives us
    // an extra sparse/dense behavior assertion for free because a sparse
    // non-match will be false in the results.
    ret[row_id - 1].valid = true;

    // Calculate the sample name length even though we know its 12.
    uint64_t name_length = 0;
    if (i < sample_name_offsets.size() - 1) {
      name_length = sample_name_offsets[i + 1] - sample_name_offsets[i];
    } else {
      name_length = table["sample_name"].second - sample_name_offsets[i];
    }

    // Make sure we're dealing with the correct cell.
    ret[row_id - 1].row_id = row_ids[i];

    // Copy over the sample name. Either the whole "AAAA00000000" id or the
    // empty string if we're on a fill value.
    ret[row_id - 1].sample_name =
        std::string(sample_names.data() + sample_name_offsets[i], name_length);

    // The cell_type attribute is non-nullable so the 255 distinguishes between
    // match and non-match for this cell.
    if (cell_types[i] == std::numeric_limits<uint8_t>::max()) {
      ret[row_id - 1].cell_type = "";
    } else {
      ret[row_id - 1].cell_type = cell_type_index_.at(cell_types[i]);
    }

    // This is a bit weird because there's null-ability logic involved with
    // cell not matching logic. One thing to keep in mind, is that we write
    // 254 as the data value when we mark a cycle phase as null. Currently
    // TileDB repeats this value back so we can use it to deduce when we wrote
    // null vs seeing an non-matching cell in the dense results.
    if (cycle_phases_validity[i]) {
      // We have a non-null cycle phase.
      ret[row_id - 1].cycle_phase = cycle_phase_index_.at(cycle_phases[i]);
      ret[row_id - 1].cycle_phase_valid = true;
    } else {
      // A null cycle phase. The assertion on cycle_phases[i] here is testing
      // our precondition that we know this should be null by the fact that
      // core returns our invalid 254 value.
      assert(cycle_phases[i] == 254);
      ret[row_id - 1].cycle_phase = "";
      ret[row_id - 1].cycle_phase_valid = false;
    }

    if (wavelengths[i] == std::numeric_limits<uint8_t>::max()) {
      // Cell didn't match, so wavelength gets the non-match value of an empty
      // string.
      ret[row_id - 1].wavelength = "";
    } else {
      ret[row_id - 1].wavelength = wavelength_index_.at(wavelengths[i]);
    }

    // This is a bit silly, but in the interest of preventing accidental
    // matches, I'm using the nanf function to give a float that is NaN with
    // the fraction as the leading digits of pi to help debugging.
    //
    // That is to say, if you start seeing NaN issues with this test, you can
    // check the fraction to see if its a "real" NaN or a logic error because
    // of how we're creating the NaN instance here.
    if (luminosities[i] == std::numeric_limits<float>::min()) {
      ret[row_id - 1].luminosity = nanf("3141592");
    } else {
      ret[row_id - 1].luminosity = luminosities[i];
    }
  }

  return ret;
}

std::vector<EnmrQCCell> CPPQueryConditionEnumerationFx::generate_data(
    uint32_t num_rows) {
  std::vector<EnmrQCCell> ret(num_rows);

  std::uniform_int_distribution<uint16_t> sn_rng(0, 9);
  std::uniform_int_distribution<uint16_t> ct_rng(
      0, static_cast<uint8_t>(cell_type_values_.size()) - 1);
  std::uniform_int_distribution<uint16_t> cp_rng(
      0, static_cast<uint8_t>(cycle_phase_values_.size()) - 1);
  std::uniform_int_distribution<uint16_t> wl_rng(
      0, static_cast<uint8_t>(wavelength_values_.size()) - 1);
  std::uniform_real_distribution<float> lum_rng(0.0, 1.0);

  std::string sample_name = "AAAA00000000";

  for (uint32_t i = 0; i < ret.size(); i++) {
    ret[i].row_id = i + 1;

    for (size_t i = 0; i < sample_name.size(); i++) {
      if (i < 4) {
        sample_name[i] = 'A' + static_cast<char>(sn_rng(rand_));
      } else {
        sample_name[i] = '0' + static_cast<char>(sn_rng(rand_));
      }
    }

    ret[i].sample_name = sample_name;
    REQUIRE(ret[i].sample_name.size() == 12);
    ret[i].cell_type = cell_type_index_.at(static_cast<uint8_t>(ct_rng(rand_)));
    // A bit hacky, but I'm reusing the luminescence RNG to make the cycle
    // phase null 30% of the time.
    if (lum_rng(rand_) < 0.3) {
      ret[i].cycle_phase = "";
      ret[i].cycle_phase_valid = false;
    } else {
      ret[i].cycle_phase =
          cycle_phase_index_.at(static_cast<uint8_t>(cp_rng(rand_)));
      ret[i].cycle_phase_valid = true;
    }
    ret[i].wavelength =
        wavelength_index_.at(static_cast<uint8_t>(wl_rng(rand_)));
    ret[i].luminosity = lum_rng(rand_);
  }

  return ret;
}

#ifdef TILEDB_SERIALIZATION
QueryCondition CPPQueryConditionEnumerationFx::serialize_deserialize_qc(
    QueryCondition& qc) {
  using namespace tiledb::sm::serialization;
  using Condition = tiledb::sm::serialization::capnp::Condition;

  auto qc_ptr = qc.ptr().get()->query_condition_;

  QueryCondition ret(ctx_);
  auto ret_ptr = ret.ptr().get()->query_condition_;

  // Serialize the query condition.
  ::capnp::MallocMessageBuilder message;
  auto builder = message.initRoot<Condition>();
  throw_if_not_ok(condition_to_capnp(*qc_ptr, &builder));

  // Deserialize the query condition.
  *ret_ptr = condition_from_capnp(builder);
  REQUIRE(tiledb::test::ast_equal(ret_ptr->ast(), qc_ptr->ast()));

  return ret;
}
#else
QueryCondition CPPQueryConditionEnumerationFx::serialize_deserialize_qc(
    QueryCondition&) {
  throw std::logic_error("Unable to serialize when serialization is disabled.");
}
#endif

void CPPQueryConditionEnumerationFx::create_enumeration(
    ArraySchema& schema,
    const std::string& name,
    const std::unordered_map<uint8_t, std::string>& index,
    bool ordered) {
  std::vector<std::string> enmr_values;
  for (uint8_t i = 0; i < static_cast<uint8_t>(index.size()); i++) {
    enmr_values.push_back(index.at(i));
  }
  auto enmr = Enumeration::create(ctx_, name, enmr_values, ordered);
  ArraySchemaExperimental::add_enumeration(ctx_, schema, enmr);
}

void CPPQueryConditionEnumerationFx::validate_query_condition(
    EnmrQCCreator creator) {
  Array array(ctx_, uri_, TILEDB_READ);
  auto core_array = array.ptr().get()->array_;
  core_array->load_all_enumerations();

  auto qc = creator(ctx_);
  auto core_qc = qc.ptr().get()->query_condition_;
  core_qc->rewrite_enumeration_conditions(core_array->array_schema_latest());

  REQUIRE(core_qc->check(core_array->array_schema_latest()).ok());
}

std::unordered_map<uint8_t, std::string>
CPPQueryConditionEnumerationFx::make_index(
    std::unordered_map<std::string, uint8_t> values) {
  std::unordered_map<uint8_t, std::string> ret;
  for (auto& [name, idx] : values) {
    assert(ret.find(idx) == ret.end());
    ret[idx] = name;
  }
  return ret;
}

void CPPQueryConditionEnumerationFx::remove_array() {
  if (vfs_.is_dir(uri_)) {
    vfs_.remove_dir(uri_);
  }
}
