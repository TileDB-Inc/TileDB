#if 0
void db_test_0(DataBlock& db) {
  auto a = db.begin();
  auto b = db.cbegin();
  auto c = db.end();
  auto d = db.cend();

  REQUIRE(a == b);
  REQUIRE(++a == ++b);
  REQUIRE(a++ == b++);
  REQUIRE(a == b);
  REQUIRE(++a != b);
  REQUIRE(a == ++b);
  REQUIRE(c == d);
  auto e = c + 5;
  auto f = d + 5;
  REQUIRE(c == e - 5);
  REQUIRE(d == f - 5);
  REQUIRE(e == f);
  REQUIRE(e - 5 == f - 5);
  auto g = a + 1;
  REQUIRE(g > a);
  REQUIRE(g >= a);
  REQUIRE(a < g);
  REQUIRE(a <= g);
}

void db_test_1(const DataBlock& db) {
  auto a = db.begin();
  auto b = db.cbegin();
  auto c = db.end();
  auto d = db.cend();

  REQUIRE(a == b);
  REQUIRE(++a == ++b);
  REQUIRE(a++ == b++);
  REQUIRE(a == b);
  REQUIRE(++a != b);
  REQUIRE(a == ++b);
  REQUIRE(c == d);
  auto e = c + 5;
  auto f = d + 5;
  REQUIRE(c == e - 5);
  REQUIRE(d == f - 5);
  REQUIRE(e == f);
  REQUIRE(e - 5 == f - 5);
  auto g = a + 1;
  REQUIRE(g > a);
  REQUIRE(g >= a);
  REQUIRE(a < g);
  REQUIRE(a <= g);
}

TEST_CASE("Ports: Test create DataBlock", "[ports]") {
  auto db = DataBlock();
  db_test_0(db);
  db_test_1(db);
}
#endif
