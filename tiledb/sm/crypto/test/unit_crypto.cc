/**
 * @file unit_crypto.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests the `Crypto` class
 */

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/crypto/crypto.h"

#include <test/support/tdb_catch.h>
#include <iomanip>
#include <iostream>

using namespace tiledb::sm;

#ifdef _WIN32
#include "tiledb/sm/crypto/crypto_win32.h"
using PlatformCrypto = Win32CNG;
#else
#include "tiledb/sm/crypto/crypto_openssl.h"
using PlatformCrypto = OpenSSL;
#endif

// We fill two 64-byte buffers with random data and check that their content
// is not the same. The probability of having the same content is vanishingly
// small.
TEST_CASE("Crypto: Test Random Number Generator", "[crypto][random]") {
  const int size = 64;
  Buffer buf1{size}, buf2{size};
  std::memset(buf1.data(), 0, buf1.alloced_size());
  std::memset(buf2.data(), 0, buf2.alloced_size());
  CHECK(PlatformCrypto::get_random_bytes(size, &buf1).ok());
  CHECK(PlatformCrypto::get_random_bytes(size, &buf2).ok());
  CHECK(buf1.size() == size);
  CHECK(buf2.size() == size);
  CHECK(std::memcmp(buf1.data(), buf2.data(), size) != 0);
}

TEST_CASE("Crypto: Test AES-256-GCM", "[crypto][aes]") {
  SECTION("- Basic") {
    unsigned nelts = 123;
    Buffer input;
    REQUIRE(input.realloc(123 * sizeof(unsigned)).ok());
    for (unsigned i = 0; i < nelts; i++)
      REQUIRE(input.write(&i, sizeof(unsigned)).ok());
    ConstBuffer input_cb(&input);

    // Set up key
    char key_bytes[] = "0123456789abcdeF0123456789abcdeF";
    ConstBuffer key(key_bytes, sizeof(key_bytes) - 1);  // -1 to ignore NUL

    // Encrypt input.
    Buffer encrypted;
    char tag_array[16], iv_array[12];
    PreallocatedBuffer output_iv(&iv_array[0], sizeof(iv_array));
    PreallocatedBuffer output_tag(&tag_array[0], sizeof(tag_array));
    CHECK(Crypto::encrypt_aes256gcm(
              &key, nullptr, &input_cb, &encrypted, &output_iv, &output_tag)
              .ok());
    CHECK(encrypted.size() == 492);

    // Check decryption.
    Buffer decrypted;
    ConstBuffer iv(output_iv.data(), output_iv.size());
    ConstBuffer tag(output_tag.data(), output_tag.size());
    ConstBuffer encrypted_cb(&encrypted);
    CHECK(Crypto::decrypt_aes256gcm(&key, &iv, &tag, &encrypted_cb, &decrypted)
              .ok());
    CHECK(decrypted.size() == input.size());
    for (unsigned i = 0; i < nelts; i++)
      REQUIRE(decrypted.value<unsigned>(i * sizeof(unsigned)) == i);

    // Check invalid tag gives error on decrypt.
    char tag_copy[16];
    memcpy(tag_copy, tag_array, sizeof(tag_array));
    memset(tag_array, 0, sizeof(tag_array));
    decrypted.reset_offset();
    decrypted.reset_size();
    CHECK(!Crypto::decrypt_aes256gcm(&key, &iv, &tag, &encrypted_cb, &decrypted)
               .ok());

    // Check with proper tag again.
    memcpy(tag_array, tag_copy, sizeof(tag_array));
    decrypted.reset_offset();
    decrypted.reset_size();
    CHECK(Crypto::decrypt_aes256gcm(&key, &iv, &tag, &encrypted_cb, &decrypted)
              .ok());
    CHECK(decrypted.size() == input.size());
    for (unsigned i = 0; i < nelts; i++)
      REQUIRE(decrypted.value<unsigned>(i * sizeof(unsigned)) == i);

    // Check incorrect key gives error.
    char key_copy[32];
    memcpy(key_copy, key_bytes, sizeof(key_bytes) - 1);
    key_bytes[0] = 'z';
    decrypted.reset_offset();
    decrypted.reset_size();
    CHECK(!Crypto::decrypt_aes256gcm(&key, &iv, &tag, &encrypted_cb, &decrypted)
               .ok());

    // Check with proper key again
    memcpy(key_bytes, key_copy, sizeof(key_copy) - 1);
    decrypted.reset_offset();
    decrypted.reset_size();
    CHECK(Crypto::decrypt_aes256gcm(&key, &iv, &tag, &encrypted_cb, &decrypted)
              .ok());
    CHECK(decrypted.size() == input.size());
    for (unsigned i = 0; i < nelts; i++)
      REQUIRE(decrypted.value<unsigned>(i * sizeof(unsigned)) == i);

    // Check wrong key length
    ConstBuffer short_key(key_bytes, 16);
    decrypted.reset_offset();
    decrypted.reset_size();
    CHECK(!Crypto::decrypt_aes256gcm(
               &short_key, &iv, &tag, &encrypted_cb, &decrypted)
               .ok());

    // Check ciphertext modification gives error.
    *((unsigned*)encrypted.value_ptr(0)) += 1;
    decrypted.reset_offset();
    decrypted.reset_size();
    CHECK(!Crypto::decrypt_aes256gcm(&key, &iv, &tag, &encrypted_cb, &decrypted)
               .ok());
  }

  SECTION("- Plaintext encrypts differently") {
    unsigned nelts = 123;
    Buffer input;
    REQUIRE(input.realloc(123 * sizeof(unsigned)).ok());
    for (unsigned i = 0; i < nelts; i++)
      REQUIRE(input.write(&i, sizeof(unsigned)).ok());
    ConstBuffer input_cb(&input);

    // Set up key
    char key_bytes[] = "0123456789abcdeF0123456789abcdeF";
    ConstBuffer key(key_bytes, sizeof(key_bytes) - 1);  // -1 to ignore NUL

    // Encrypt the same plaintext twice.
    Buffer encrypted, encrypted2;
    char tag_array[16], iv_array[12];
    PreallocatedBuffer output_iv(&iv_array[0], sizeof(iv_array));
    PreallocatedBuffer output_tag(&tag_array[0], sizeof(tag_array));
    CHECK(Crypto::encrypt_aes256gcm(
              &key, nullptr, &input_cb, &encrypted, &output_iv, &output_tag)
              .ok());
    CHECK(Crypto::encrypt_aes256gcm(
              &key, nullptr, &input_cb, &encrypted2, &output_iv, &output_tag)
              .ok());

    // Check encrypted bytes are different.
    CHECK(encrypted.size() == encrypted2.size());
    bool all_same = true;
    for (unsigned i = 0; i < encrypted.size(); i++)
      all_same &= encrypted.value<uint8_t>(i) == encrypted2.value<uint8_t>(i);
    CHECK(!all_same);

    // Check decryption.
    Buffer decrypted;
    ConstBuffer iv(output_iv.data(), output_iv.size());
    ConstBuffer tag(output_tag.data(), output_tag.size());
    ConstBuffer encrypted_cb(&encrypted2);
    CHECK(Crypto::decrypt_aes256gcm(&key, &iv, &tag, &encrypted_cb, &decrypted)
              .ok());
    CHECK(decrypted.size() == input.size());
    for (unsigned i = 0; i < nelts; i++)
      REQUIRE(decrypted.value<unsigned>(i * sizeof(unsigned)) == i);
  }

  SECTION("- Different input lengths") {
    std::vector<unsigned> nelt_values = {0, 1, 100, 1231, 1000, 10000};
    for (unsigned nelts : nelt_values) {
      Buffer input;
      REQUIRE(input.realloc(123 * sizeof(unsigned)).ok());
      for (unsigned i = 0; i < nelts; i++)
        REQUIRE(input.write(&i, sizeof(unsigned)).ok());
      ConstBuffer input_cb(&input);

      // Set up key
      char key_bytes[] = "0123456789abcdeF0123456789abcdeF";
      ConstBuffer key(key_bytes, sizeof(key_bytes) - 1);  // -1 to ignore NUL

      // Encrypt input.
      Buffer encrypted;
      char tag_array[16], iv_array[12];
      PreallocatedBuffer output_iv(&iv_array[0], sizeof(iv_array));
      PreallocatedBuffer output_tag(&tag_array[0], sizeof(tag_array));
      CHECK(Crypto::encrypt_aes256gcm(
                &key, nullptr, &input_cb, &encrypted, &output_iv, &output_tag)
                .ok());
      CHECK(encrypted.size() == input.size());

      // Check decryption.
      Buffer decrypted;
      ConstBuffer iv(output_iv.data(), output_iv.size());
      ConstBuffer tag(output_tag.data(), output_tag.size());
      ConstBuffer encrypted_cb(&encrypted);
      CHECK(
          Crypto::decrypt_aes256gcm(&key, &iv, &tag, &encrypted_cb, &decrypted)
              .ok());
      CHECK(decrypted.size() == input.size());
      for (unsigned i = 0; i < nelts; i++)
        REQUIRE(decrypted.value<unsigned>(i * sizeof(unsigned)) == i);
    }
  }

  SECTION("- NIST test vectors") {
    // From:
    // https://csrc.nist.gov/Projects/Cryptographic-Algorithm-Validation-Program/CAVP-TESTING-BLOCK-CIPHER-MODES#GCMVS
    //
    // These are test vectors where:
    // Keylen = 256, IVlen = 96, PTlen = 408, AADlen = 0, Taglen = 128.
    struct TestCase {
      char key[64 + 1];
      char iv[24 + 1];
      char pt[102 + 1];
      char ct[102 + 1];
      char tag[32 + 1];

      TestCase(
          const char* key_arg,
          const char* iv_arg,
          const char* pt_arg,
          const char* ct_arg,
          const char* tag_arg) {
        std::memcpy(key, key_arg, sizeof(key));
        std::memcpy(iv, iv_arg, sizeof(iv));
        std::memcpy(pt, pt_arg, sizeof(pt));
        std::memcpy(ct, ct_arg, sizeof(ct));
        std::memcpy(tag, tag_arg, sizeof(tag));
      }

      Buffer get_buffer(
          unsigned buf_size, unsigned num_chars, const char* field) const {
        Buffer result;
        REQUIRE(result.realloc(buf_size).ok());
        for (unsigned i = 0; i < num_chars; i += 2) {
          char byte_str[3] = {field[i], field[i + 1], '\0'};
          auto byte = (uint8_t)strtoul(byte_str, nullptr, 16);
          REQUIRE(result.write(&byte, sizeof(uint8_t)).ok());
        }
        return result;
      }

      Buffer get_key() const {
        return get_buffer(256 / 8, 64, key);
      }

      Buffer get_iv() const {
        return get_buffer(96 / 8, 24, iv);
      }

      Buffer get_plaintext() const {
        return get_buffer(408 / 8, 102, pt);
      }

      Buffer get_tag() const {
        return get_buffer(128 / 8, 32, tag);
      }

      Buffer get_ciphertext() const {
        return get_buffer(408 / 8, 102, ct);
      }
    };

    std::vector<TestCase> tests = {
        TestCase(
            "1fded32d5999de4a76e0f8082108823aef60417e1896cf4218a2fa90f632ec8a",
            "1f3afa4711e9474f32e70462",
            "06b2c75853df9aeb17befd33cea81c630b0fc53667ff45199c629c8e15dce41e53"
            "0aa"
            "792f796b8138eeab2e86c7b7bee1d40b0",
            "91fbd061ddc5a7fcc9513fcdfdc9c3a7c5d4d64cedf6a9c24ab8a77c36eefbf1c5"
            "dc0"
            "0bc50121b96456c8cd8b6ff1f8b3e480f",
            "30096d340f3d5c42d82a6f475def23eb"),

        TestCase(
            "b405ac89724f8b555bfee1eaa369cd854003e9fae415f28c5a199d4d6efc83d6",
            "cec71a13b14c4d9bd024ef29",
            "ab4fd35bef66addfd2856b3881ff2c74fdc09c82abe339f49736d69b2bd0a71a6b"
            "4fe"
            "8fc53f50f8b7d6d6d6138ab442c7f653f",
            "69a079bca9a6a26707bbfa7fd83d5d091edc88a7f7ff08bd8656d8f2c92144ff23"
            "400"
            "fcb5c370b596ad6711f386e18f2629e76",
            "6d2b7861a3c59ba5a3e3a11c92bb2b14"),

        TestCase(
            "fad40c82264dc9b8d9a42c10a234138344b0133a708d8899da934bfee2bdd6b8",
            "0dade2c95a9b85a8d2bc13ef",
            "664ea95d511b2cfdb9e5fb87efdd41cbfb88f3ff47a7d2b8830967e39071a89b94"
            "875"
            "4ffb0ed34c357ed6d4b4b2f8a76615c03",
            "ea94dcbf52b22226dda91d9bfc96fb382730b213b66e30960b0d20d2417036cbaa"
            "9e3"
            "59984eea947232526e175f49739095e69",
            "5ca8905d469fffec6fba7435ebdffdaf"),

        TestCase(
            "aa5fca688cc83283ecf39454679948f4d30aa8cb43db7cc4da4eff1669d6c52f",
            "4b2d7b699a5259f9b541fa49",
            "c691f3b8f3917efb76825108c0e37dc33e7a8342764ce68a62a2dc1a5c94059496"
            "1fc"
            "d5c0df05394a5c0fff66c254c6b26a549",
            "2cd380ebd6b2cf1b80831cff3d6dc2b6770778ad0d0a91d03eb8553696800f8431"
            "1d3"
            "37302519d1036feaab8c8eb845882c5f0",
            "5de4ef67bf8896fbe82c01dca041d590"),

        TestCase(
            "1c7690d5d845fceabba227b11ca221f4d6d302233641016d9cd3a158c3e36017",
            "93bca8de6b11a4830c5f5f64",
            "3c79a39878a605f3ac63a256f68c8a66369cc3cd7af680d19692b485a7ba58ce1d"
            "536"
            "707c55eda5b256c8b29bbf0b4cbeb4fc4",
            "c9e48684df13afccdb1d9ceaa483759022e59c3111188c1eceb02eaf308035b042"
            "8db"
            "826de862d925a3c55af0b61fd8f09a74d",
            "8f577e8730c19858cad8e0124f311dd9"),

        TestCase(
            "dbdb5132f126e62ce5b74bf85a2ac33b276588a3fc91d1bb5c7405a1bf68418b",
            "64f9e16489995e1a99568118",
            "b2740a3d5647aa5aaeb98a2e7bbf31edaea1ebacd63ad96b4e2688f1ff08af8ee4"
            "071"
            "bf26941c517d74523668ca1f9dfdbcaab",
            "e5fec362d26a1286b7fd2ec0fa876017437c7bce242293ff03d72c2f321d9e3931"
            "6a6"
            "aa7404a65ccd84890c2f527c1232b58d5",
            "dfa591ee2372699758d2cc43bfcbd2ba"),

        TestCase(
            "8433a85f16c7c921476c83d042cb713eb11a83fc0cffe31dde97907f060b4ee9",
            "55ffc85ffd1cdea8b8c48382",
            "23bc3983ba5b3be91c8a6aa148a99995241ee9e82ce44e1184beb742affbe48f54"
            "5c9"
            "a980480cf1fab758a46e4711ea9267466",
            "2f4bdc7b8b8cec1863e3145871554778c43963b527f8413bb9779935c138a34d86"
            "d7c"
            "76a9e6af689902f316191e12f34126a42",
            "7dc63156b12c9868e6b9a5843df2d79e"),

        TestCase(
            "5d7bf55457929c65e4f2a97cbdcc9b432405b1352451ccc958bceebce557491d",
            "f45ae70c264ed6e1cc132978",
            "ba5ac2a16d84b0df5a6e40f097d9d44bf21de1fcec06e4c7857463963e5c65c936"
            "d37"
            "d78867f253ce25690811bf39463e5702a",
            "47c16f87ebf00ba3e50416b44b99976c2db579423c3a3420479c477cd5ef57621c"
            "9c0"
            "cee7520acb55e739cc5435bc8665a2a0c",
            "456054ecb55cf7e75f9543def2c6e98c"),

        TestCase(
            "595f259c55abe00ae07535ca5d9b09d6efb9f7e9abb64605c337acbd6b14fc7e",
            "92f258071d79af3e63672285",
            "a6fee33eb110a2d769bbc52b0f36969c287874f665681477a25fc4c48015c541fb"
            "e23"
            "94133ba490a34ee2dd67b898177849a91",
            "bbca4a9e09ae9690c0f6f8d405e53dccd666aa9c5fa13c8758bc30abe1ddd1bcce"
            "0d3"
            "6a1eaaaaffef20cd3c5970b9673f8a65c",
            "26ccecb9976fd6ac9c2c0f372c52c821"),

        TestCase(
            "251227f72c481a7e064cbbaa5489bc85d740c1e6edea2282154507877ed56819",
            "db7193d9cd7aeced99062a1c",
            "cccffd58fded7e589481da18beec51562481f4b28c2944819c37f7125d56dceca0"
            "ef0"
            "bb6f7d7eeb5b7a2bd6b551254e9edff3a",
            "1cc08d75a03d32ee9a7ae88e0071406dbee1c306383cf41731f3c547f3377b92f7"
            "cc2"
            "8b3c1066601f54753fbd689af5dbc5448",
            "a0c7b7444229a8cfef24a31ee2de9961"),

        TestCase(
            "f256504fc78fff7139c42ed1510edf9ac5de27da706401aa9c67fd982d435911",
            "8adcf2d678abcef9dd45e8f9",
            "d1b6db2b2c81751170d9e1a39997539e3e926ca4a43298cdd3eb6fe8678b508cdb"
            "90a"
            "8a94171abe2673894405eda5977694d7a",
            "76205d63b9c5144e5daa8ac7e51f19fa96e71a3106ab779b67a8358ab5d60ef771"
            "977"
            "06266e2c214138334a3ed66ceccb5a6cd",
            "c1fe53cf85fbcbff932c6e1d026ea1d5"),

        TestCase(
            "21d296335f58515a90537a6ca3a38536eba1f899a2927447a3be3f0add70bea5",
            "2be3ad164fcbcf8ee6708535",
            "ad278650092883d348be63e991231ef857641e5efc0cab9bb28f360becc3c103d2"
            "794"
            "785024f187beaf9665b986380c92946a7",
            "b852aeba704e9d89448ba180a0bfde9e975a21cc073d0c02701215872ed7469f00"
            "fe3"
            "49294ba2d72bf3c7780b72c76101ba148",
            "bdd6d708b45ae54cd8482e4c5480a3c1"),

        TestCase(
            "d42380580e3491ddfbc0ec32424e3a281cbe71aa7505ff5ab8d24e64fbe47518",
            "fbed88de61d605a7137ffeb2",
            "4887a6ef947888bf80e4c40d9769650506eb4f4a5fd241b42c9046e3a2cf119db0"
            "02f"
            "89a9eba1d11b7a378be6b27d6f8fc86c9",
            "87aa27f96187ce27e26caf71ba5ba4e37705fd86ca9291ea68d6c6f9030291cdbf"
            "f58"
            "bff1e6741590b268367e1f1b8c4b94cd4",
            "d1690a6fe403c4754fd3773d89395ecd"),

        TestCase(
            "5511727ecd92acec510d5d8c0c49b3caacd2140431cf51e09437ebd8ca82e2ce",
            "ae80d03696e23464c881ccff",
            "184b086646ef95111ccb3d319f3124f4d4d241f9d731ce26662ea39e43457e30b0"
            "bd7"
            "39b5d5dbceb353ce0c3647a3a4c87e3b0",
            "aa28cb257698963dfc3e3fe86368d881ac066eb8ee215a7c0ed72e4d081db0b940"
            "071"
            "e2e64ff6204960da8e3464daf4cb7f37b",
            "c1578aa6e3325ee4b5e9fb9ee62a7028"),

        TestCase(
            "d48f3072bbd535a2df0a2864feb33b488596cd523ad1623b1cefe7b8cbefcf4a",
            "bbf2a537d285444d94f5e944",
            "060c585bd51539afdd8ff871440db36bfdce33b7f039321b0a63273a318bd25375"
            "a2d"
            "9615b236cfe63d627c6c561535ddfb6bd",
            "993d5d692c218570d294ab90d5f7aa683dc0e470efac279a776040f3b49386813f"
            "68b"
            "0db6a7aef59025cc38520fb318a1eac55",
            "8cd808438a8f5b6a69ff3ae255bf2cb2")};

    // Run all test vectors
    for (auto& test : tests) {
      auto key = test.get_key();
      auto iv = test.get_iv();
      auto plaintext = test.get_plaintext();
      auto tag_answer = test.get_tag();
      auto ciphertext_answer = test.get_ciphertext();
      REQUIRE(key.size() * 8 == 256);
      REQUIRE(iv.size() * 8 == 96);
      REQUIRE(plaintext.size() * 8 == 408);
      REQUIRE(tag_answer.size() * 8 == 128);
      REQUIRE(ciphertext_answer.size() * 8 == 408);

      // Encrypt plaintext
      char tag_array[16], iv_array[12];
      PreallocatedBuffer output_tag(&tag_array[0], sizeof(tag_array));
      PreallocatedBuffer output_iv(&iv_array[0], sizeof(iv_array));
      ConstBuffer key_const(key.data(), key.size());
      ConstBuffer iv_const(iv.data(), iv.size());
      Buffer encrypted;
      ConstBuffer plaintext_cb(&plaintext);
      CHECK(Crypto::encrypt_aes256gcm(
                &key_const,
                &iv_const,
                &plaintext_cb,
                &encrypted,
                &output_iv,
                &output_tag)
                .ok());

      // Check answer.
      for (unsigned i = 0; i < 16; i++)
        CHECK(
            output_tag.value<char>(i * sizeof(char)) ==
            tag_answer.value<char>(i * sizeof(char)));
      for (unsigned i = 0; i < 12; i++)
        CHECK(
            output_iv.value<char>(i * sizeof(char)) ==
            iv.value<char>(i * sizeof(char)));
      CHECK(encrypted.size() == ciphertext_answer.size());
      for (unsigned i = 0; i < ciphertext_answer.size(); i++)
        CHECK(
            encrypted.value<char>(i * sizeof(char)) ==
            ciphertext_answer.value<char>(i * sizeof(char)));

      // Decrypt and check answer.
      Buffer decrypted;
      ConstBuffer tag_const(output_tag.data(), output_tag.size());
      ConstBuffer encrypted_cb(&encrypted);
      CHECK(Crypto::decrypt_aes256gcm(
                &key_const, &iv_const, &tag_const, &encrypted_cb, &decrypted)
                .ok());
      CHECK(decrypted.size() == plaintext.size());
      for (unsigned i = 0; i < decrypted.size(); i++)
        CHECK(
            decrypted.value<char>(i * sizeof(char)) ==
            plaintext.value<char>(i * sizeof(char)));
    }
  }
}

static std::vector<uint8_t> from_hex(const std::string& str) {
  std::vector<uint8_t> vec;
  for (size_t i = 0; i < str.length(); i += 2) {
    char byte_str[3] = {str[i], str[i + 1], '\0'};
    vec.push_back((uint8_t)std::strtoul(byte_str, nullptr, 16));
  }
  return vec;
}

static std::string to_hex(const uint8_t* data, uint64_t length) {
  std::stringstream ss;
  for (uint64_t i = 0; i < length; i++) {
    ss << std::hex << std::setw(2) << std::setfill('0') << (int)data[i];
  }
  return ss.str();
}

// Test that the given input has the expected hash value in
// hex. The function is generic over the hash and the input type.
template <class Hash>
static void test_hash(
    const uint8_t* input, uint64_t length, const std::string& expected_hash) {
  REQUIRE(expected_hash.length() % 2 == 0);

  Buffer hash_buf(Hash::digest_bytes);
  CHECK((Hash::hash(input, length, &hash_buf)).ok());

  // Compare the strings for a better error message in case of failure.
  CHECK(
      to_hex((uint8_t*)hash_buf.data(), hash_buf.alloced_size()) ==
      expected_hash);
}

struct MD5Hash {
  static const int digest_bytes = Crypto::MD5_DIGEST_BYTES;
  static Status hash(const uint8_t* input, uint64_t length, Buffer* output) {
    return Crypto::md5(input, length, output);
  }
};

TEST_CASE("Crypto: Test MD5", "[crypto][md5]") {
  auto test_md5 = [](const std::string input, const std::string expected_hash) {
    test_hash<MD5Hash>((uint8_t*)(input.data()), input.length(), expected_hash);
  };
  // Values taken from section A.5 in https://www.ietf.org/rfc/rfc1321.txt
  test_md5("", "d41d8cd98f00b204e9800998ecf8427e");
  test_md5("a", "0cc175b9c0f1b6a831c399e269772661");
  test_md5("abc", "900150983cd24fb0d6963f7d28e17f72");
  test_md5("message digest", "f96b697d7cb7938d525a2f31aaf161d0");
  test_md5("abcdefghijklmnopqrstuvwxyz", "c3fcd3d76192e4007dfb496cca67e13b");
  test_md5(
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
      "d174ab98d277d9f5a5611c2c9f419d9f");
  test_md5(
      "1234567890123456789012345678901234567890123456789012345678901234567890"
      "1234567890",
      "57edf4a22be3c955ac49da2e2107b67a");
}

struct SHA256Hash {
  static const int digest_bytes = Crypto::SHA256_DIGEST_BYTES;
  static Status hash(const uint8_t* input, uint64_t length, Buffer* output) {
    return Crypto::sha256(input, length, output);
  }
};

TEST_CASE("Crypto: Test SHA256", "[crypto][sha256]") {
  auto test_sha256 = [](const std::string& input,
                        const std::string& expected_hash) {
    auto data = from_hex(input);
    test_hash<SHA256Hash>(data.data(), data.size(), expected_hash);
  };
  // Values taken from SHA256ShortMsg.rsp, from "SHA Test Vectors for Hashing
  // Bit-Oriented Messages", found at
  // https://csrc.nist.gov/Projects/cryptographic-algorithm-validation-program/Secure-Hashing
  // Len = 0
  test_sha256(
      "", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  // Len = 64
  test_sha256(
      "5738c929c4f4ccb6",
      "963bb88f27f512777aab6c8b1a02c70ec0ad651d428f870036e1917120fb48bf");
  // Len = 128
  test_sha256(
      "0a27847cdc98bd6f62220b046edd762b",
      "80c25ec1600587e7f28b18b1b18e3cdc89928e39cab3bc25e4d4a4c139bcedc4");
  // Len = 192
  test_sha256(
      "47991301156d1d977c0338efbcad41004133aefbca6bcf7e",
      "feeb4b2b59fec8fdb1e55194a493d8c871757b5723675e93d3ac034b380b7fc9");
  // Len = 256
  test_sha256(
      "09fc1accc230a205e4a208e64a8f204291f581a12756392da4b8c0cf5ef02b95",
      "4f44c1c7fbebb6f9601829f3897bfd650c56fa07844be76489076356ac1886a4");
  // Len = 384
  test_sha256(
      "4eef5107459bddf8f24fc7656fd4896da8711db50400c0164847f692b886ce8d7f4d67"
      "395090b3534efd7b0d298da34b",
      "7c5d14ed83dab875ac25ce7feed6ef837d58e79dc601fb3c1fca48d4464e8b83");
  // Len = 448
  test_sha256(
      "2d52447d1244d2ebc28650e7b05654bad35b3a68eedc7f8515306b496d75f3e73385dd"
      "1b002625024b81a02f2fd6dffb6e6d561cb7d0bd7a",
      "cfb88d6faf2de3a69d36195acec2e255e2af2b7d933997f348e09f6ce5758360");
  // Len = 512
  test_sha256(
      "5a86b737eaea8ee976a0a24da63e7ed7eefad18a101c1211e2b3650c5187c2a8a65054"
      "7208251f6d4237e661c7bf4c77f335390394c37fa1a9f9be836ac28509",
      "42e61e174fbb3897d6dd6cef3dd2802fe67b331953b06114a65c772859dfc1aa");
}
