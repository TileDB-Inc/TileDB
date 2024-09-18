/**
 * @file   AWSCredentialsProviderChain.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file defines a vendored copy of the
 * Aws::Auth::DefaultAWSCredentialsProviderChain class, updated to support
 * getting a client configuration object.
 *
 * Changes made should be contributed upstream to the AWS SDK for C++ and when
 * that happens, the vendored copy should be removed.
 */

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <memory>

namespace tiledb::sm::filesystem::s3 {
/**
 * Creates an AWSCredentialsProviderChain which uses in order
 * EnvironmentAWSCredentialsProvider, ProfileConfigFileAWSCredentialsProvider,
 * ProcessCredentialsProvider, STSAssumeRoleWebIdentityCredentialsProvider and
 * SSOCredentialsProvider.
 */
class DefaultAWSCredentialsProviderChain
    : public Aws::Auth::AWSCredentialsProviderChain {
 public:
  /**
   * Initializes the provider chain with EnvironmentAWSCredentialsProvider,
   * ProfileConfigFileAWSCredentialsProvider, ProcessCredentialsProvider,
   * STSAssumeRoleWebIdentityCredentialsProvider and SSOCredentialsProvider in
   * that order.
   *
   * @param clientConfig Optional client configuration to use.
   */
  DefaultAWSCredentialsProviderChain(
      std::shared_ptr<const Aws::Client::ClientConfiguration> clientConfig =
          nullptr);

  DefaultAWSCredentialsProviderChain(
      const DefaultAWSCredentialsProviderChain& chain);
};

}  // namespace tiledb::sm::filesystem::s3
