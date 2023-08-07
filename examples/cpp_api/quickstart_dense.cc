#include <chrono>
#include <ctime>
#include <iostream>
#include <thread>
#include <tiledb/tiledb>
#include <vector>

using namespace tiledb;

#define SIZE 1024
#define MAX_VAL 100000
#define NUM_THREADS 10

std::string array_name_("quickstart_dense_array");

Context ctx_;

void create_array() {
  Domain domain(ctx_);
  domain.add_dimension(Dimension::create<int>(ctx_, "rows", {{1, SIZE}}, 4))
      .add_dimension(Dimension::create<int>(ctx_, "cols", {{1, SIZE}}, 4));

  ArraySchema schema(ctx_, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  schema.add_attribute(Attribute::create<int>(ctx_, "r"));
  schema.add_attribute(Attribute::create<int>(ctx_, "g"));
  schema.add_attribute(Attribute::create<int>(ctx_, "b"));

  Array::create(array_name_, schema);
}

void write_image() {
  std::vector<int> r;
  r.reserve(SIZE * SIZE);
  std::vector<int> g;
  g.reserve(SIZE * SIZE);
  std::vector<int> b;
  b.reserve(SIZE * SIZE);
  for (int i = 0; i < SIZE; i++) {
    for (int j = 0; j < SIZE; j++) {
      r.emplace_back(rand() % MAX_VAL);
      g.emplace_back(rand() % MAX_VAL);
      b.emplace_back(rand() % MAX_VAL);
    }
  }

  Array array(ctx_, array_name_, TILEDB_WRITE);
  Query query(ctx_, array, TILEDB_WRITE);
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("r", r)
      .set_data_buffer("g", g)
      .set_data_buffer("b", b);

  query.submit();
  array.close();
}

int iter = 0;

std::vector<int> read_r(SIZE* SIZE* NUM_THREADS);
std::vector<int> read_g(SIZE* SIZE* NUM_THREADS);
std::vector<int> read_b(SIZE* SIZE* NUM_THREADS);

Array* read_array;

void read_image(int thread_id) {
  std::this_thread::sleep_for(std::chrono::microseconds(rand() % 1000));

  Subarray subarray(ctx_, *read_array);
  subarray.add_range(0, 1, 100).add_range(1, 1, 100);

  Query query(ctx_, *read_array, TILEDB_READ);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer(
          "r", read_r.data() + SIZE * SIZE * thread_id, SIZE * SIZE)
      .set_data_buffer(
          "g", read_g.data() + SIZE * SIZE * thread_id, SIZE * SIZE)
      .set_data_buffer(
          "b", read_b.data() + SIZE * SIZE * thread_id, SIZE * SIZE);

  query.submit();

  if (thread_id == 0) {
    auto end = std::chrono::system_clock::now();
    std::time_t end_time = std::chrono::system_clock::to_time_t(end);
    std::cout << "Query done it: " << iter++ << " at: " << std::ctime(&end_time)
              << std::endl;
  }
}

void read_thread(int thread_id) {
  while (true) {
    read_image(thread_id);
  }
}

int main() {
  if (Object::object(ctx_, array_name_).type() != Object::Type::Array) {
    create_array();
    for (int i = 0; i < 10; i++) {
      write_image();
    }
  }

  Array array(ctx_, array_name_, TILEDB_READ);
  read_array = &array;

  std::vector<std::thread> threads;
  for (int i = 0; i < NUM_THREADS; i++) {
    threads.emplace_back(read_thread, i);
  }

  for (auto& t : threads) {
    t.join();
  }

  array.close();

  return 0;
}
