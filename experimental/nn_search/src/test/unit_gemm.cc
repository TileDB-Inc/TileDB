/**
 * @file   unit_gemm.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 */

#include <catch2/catch_all.hpp>
#include <span>
#include <vector>
#include "../defs.h"

// If apple, use Accelerate
#ifdef __APPLE__
#include <Accelerate/Accelerate.h>
#else
// If not apple, use OpenBLAS
#include <cblas.h>
#endif

/*
      func cblas_dgemm(
              _ __Order: CBLAS_ORDER,
                _ __TransA: CBLAS_TRANSPOSE,
                _ __TransB: CBLAS_TRANSPOSE,
                _ __M: Int32,
                _ __N: Int32,
                _ __K: Int32,
                _ __alpha: Double,
                _ __A: UnsafePointer<Double>!,
                _ __lda: Int32,
                _ __B: UnsafePointer<Double>!,
                _ __ldb: Int32,
                _ __beta: Double,
                _ __C: UnsafeMutablePointer<Double>!,
                _ __ldc: Int32
              )
*/

TEST_CASE("gemm: test test", "[gemm]") {
  float a{0};
  cblas_sgemm(
      CblasRowMajor,
      CblasNoTrans,
      CblasNoTrans,
      1,
      1,
      1,
      1.0,
      &a,
      1,
      &a,
      1,
      1.0,
      &a,
      1);
}

TEST_CASE("gemm: row major test", "[sgemm]") {
  std::vector<float> A{1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
  std::vector<float> B{7.0, 8.0, 9.0, 10.0, 11.0, 12.0};

  SECTION("Row: A is 2 X 3, B is 3 X 2, C is 2 X 2") {
    // 58 64 139 154
    std::vector<float> C{0.0, 0.0, 0.0, 0.0};

    cblas_sgemm(
        CblasRowMajor,
        CblasNoTrans,
        CblasNoTrans,
        2,
        2,
        3,
        1.0,
        &A[0],
        3,
        &B[0],
        2,
        0.0,
        &C[0],
        2);
    CHECK(C[0] == 58.0);
    CHECK(C[1] == 64.0);
    CHECK(C[2] == 139.0);
    CHECK(C[3] == 154.0);
  }
  SECTION("Col: A is 2 X 3, B is 3 X 2, C is 2 X 2") {
    // 76 103 100 136
    std::vector<float> C{0.0, 0.0, 0.0, 0.0};

    cblas_sgemm(
        CblasColMajor,
        CblasNoTrans,
        CblasNoTrans,
        2,
        2,
        3,
        1.0,
        &A[0],
        2,
        &B[0],
        3,
        0.0,
        &C[0],
        2);
    CHECK(C[0] == 76.0);
    CHECK(C[1] == 100.0);
    CHECK(C[2] == 103.0);
    CHECK(C[3] == 136.0);
  }
  SECTION("Row w/spans: A is 2 X 3, B is 3 X 2, C is 2 X 2") {
    // 58 64 139 154
    std::vector<float> C{0.0, 0.0, 0.0, 0.0};
    std::vector<std::span<float>> A_span(2);
    std::vector<std::span<float>> B_span(3);
    std::vector<std::span<float>> C_span(2);
    for (size_t i = 0; i < 2; ++i) {
      A_span[i] = std::span<float>(&A[i * 3], 3);
      C_span[i] = std::span<float>(&C[i * 2], 2);
    }
    for (size_t j = 0; j < 3; ++j) {
      B_span[j] = std::span<float>(&B[j * 2], 2);
    }

    float k = 1.0;
    for (size_t i = 0; i < 2; ++i) {
      for (size_t j = 0; j < 3; ++j) {
        A_span[i][j] = k;
        k += 1.0;
      }
    }
    for (size_t i = 0; i < 3; ++i) {
      for (size_t j = 0; j < 2; ++j) {
        B_span[i][j] = k;
        k += 1.0;
      }
    }
    CHECK(A_span[0][0] == 1.0);
    CHECK(A_span[0][1] == 2.0);
    CHECK(A_span[0][2] == 3.0);

    cblas_sgemm(
        CblasRowMajor,
        CblasNoTrans,
        CblasNoTrans,
        2,
        2,
        3,
        1.0,
        &A[0],
        3,
        &B[0],
        2,
        0.0,
        &C[0],
        2);
    CHECK(C_span[0][0] == 58.0);
    CHECK(C_span[0][1] == 64.0);
    CHECK(C_span[1][0] == 139.0);
    CHECK(C_span[1][1] == 154.0);
  }
  SECTION("Col w/span: A is 2 X 3, B is 3 X 2, C is 2 X 2") {
    // 76 103 100 136
    std::vector<float> C{0.0, 0.0, 0.0, 0.0};
    std::vector<std::span<float>> A_span(3);
    std::vector<std::span<float>> B_span(2);
    std::vector<std::span<float>> C_span(2);
    for (size_t i = 0; i < 2; ++i) {
      B_span[i] = std::span<float>(&B[i * 3], 3);
      C_span[i] = std::span<float>(&C[i * 2], 2);
    }
    for (size_t j = 0; j < 3; ++j) {
      A_span[j] = std::span<float>(&A[j * 2], 2);
    }

    float k = 1.0;
    for (size_t j = 0; j < 3; ++j) {
      for (size_t i = 0; i < 2; ++i) {
        A_span[j][i] = k;
        k += 1.0;
      }
    }
    for (size_t j = 0; j < 2; ++j) {
      for (size_t i = 0; i < 3; ++i) {
        B_span[j][i] = k;
        k += 1.0;
      }
    }
    CHECK(A_span[0][0] == 1.0);
    CHECK(A_span[0][1] == 2.0);
    CHECK(A_span[0][2] == 3.0);

    cblas_sgemm(
        CblasColMajor,
        CblasNoTrans,
        CblasNoTrans,
        2,
        2,
        3,
        1.0,
        &A[0],
        2,
        &B[0],
        3,
        0.0,
        &C[0],
        2);
    CHECK(C_span[0][0] == 76.0);
    CHECK(C_span[1][0] == 103.0);
    CHECK(C_span[0][1] == 100.0);
    CHECK(C_span[1][1] == 136.0);
  }
  SECTION("Col w/span, L2: A is 3 X 2, B is 3 X 2, C is 2 X 2") {
    std::vector<float> C{0.0, 0.0, 0.0, 0.0};
    std::vector<float> L{0.0, 0.0, 0.0, 0.0};

    std::vector<std::span<float>> A_span(3);
    std::vector<std::span<float>> B_span(3);
    std::vector<std::span<float>> C_span(2);
    std::vector<std::span<float>> L2_span(2);

    std::vector<float> alpha(2);
    std::vector<float> beta(2);

    for (size_t i = 0; i < 2; ++i) {
      A_span[i] = std::span<float>(&A[i * 3], 3);
      B_span[i] = std::span<float>(&B[i * 3], 3);
      C_span[i] = std::span<float>(&C[i * 2], 2);
      L2_span[i] = std::span<float>(&L[i * 2], 2);
    }

    float k = 1.0;
    for (size_t j = 0; j < 2; ++j) {
      for (size_t i = 0; i < 3; ++i) {
        A_span[j][i] = k;
        k += 1.0;
      }
    }
    for (size_t j = 0; j < 2; ++j) {
      for (size_t i = 0; i < 3; ++i) {
        B_span[j][i] = k;
        k += 1.0;
      }
    }
    CHECK(A_span[0][0] == 1.0);
    CHECK(A_span[0][1] == 2.0);
    CHECK(A_span[0][2] == 3.0);

    /****************************************************************
     *
     *  Compute L2 distance between each column of A and each column of B
     *
     *****************************************************************/
    for (size_t i = 0; i < 2; ++i) {
      for (size_t j = 0; j < 2; ++j) {
        L2_span[j][i] = L2(B_span[j], A_span[i]);
      }
    }
    CHECK(std::abs(L2_span[0][0] - 10.3923) < .0001);
    CHECK(std::abs(L2_span[1][0] - 15.5884) < .0001);
    CHECK(std::abs(L2_span[0][1] - 5.1961) < .0001);
    CHECK(std::abs(L2_span[1][1] - 10.3923) < .0001);

    /****************************************************************
     *
     *  Use gemm to compute L2 distances between columns of A and B
     *
     *****************************************************************/

    col_sum(A_span, alpha, [](float x) { return x * x; });
    col_sum(B_span, beta, [](float x) { return x * x; });
    CHECK(alpha[0] == 14.0);
    CHECK(alpha[1] == 77.0);

    CHECK(beta[0] == 194.0);
    CHECK(beta[1] == 365.0);

    for (size_t j = 0; j < 2; ++j) {
      for (size_t i = 0; i < 2; ++i) {
        C_span[j][i] = alpha[i] + beta[j];
      }
    }

    cblas_sgemm(
        CblasColMajor,
        CblasTrans,
        CblasNoTrans,
        2,
        2,
        3,
        -2.0,
        &A[0],
        3,
        &B[0],
        3,
        1.0,
        &C[0],
        2);
    for (size_t j = 0; j < 2; ++j) {
      for (size_t i = 0; i < 2; ++i) {
        C_span[j][i] = sqrt(C_span[j][i]);
      }
    }
    CHECK(std::abs(C_span[0][0] - 10.3923) < .0001);
    CHECK(std::abs(C_span[1][0] - 15.5884) < .0001);
    CHECK(std::abs(C_span[0][1] - 5.1961) < .0001);
    CHECK(std::abs(C_span[1][1] - 10.3923) < .0001);
  }
}
