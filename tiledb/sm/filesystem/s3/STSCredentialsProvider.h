/**
 * @file   STSCredentialsProvider.h
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
 * Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider class, updated to
 * support getting a client configuration object.
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
#include <aws/core/internal/AWSHttpResourceClient.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <memory>
#include "tiledb/common/common.h"

namespace tiledb::sm::filesystem::s3 {
/**
 * To support retrieving credentials of STS AssumeRole with web identity.
 * Note that STS accepts request with protocol of queryxml. Calling
 * GetAWSCredentials() will trigger (if expired) a query request using
 * AWSHttpResourceClient under the hood.
 */
class STSAssumeRoleWebIdentityCredentialsProvider
    : public Aws::Auth::AWSCredentialsProvider {
 public:
  STSAssumeRoleWebIdentityCredentialsProvider();

  /**
   * Retrieves the credentials if found, otherwise returns empty credential set.
   */
  Aws::Auth::AWSCredentials GetAWSCredentials() override;

 protected:
  void Reload() override;

 private:
  void RefreshIfExpired();
  Aws::String CalculateQueryString() const;

  tdb_unique_ptr<Aws::Internal::STSCredentialsClient> m_client;
  Aws::Auth::AWSCredentials m_credentials;
  Aws::String m_roleArn;
  Aws::String m_tokenFile;
  Aws::String m_sessionName;
  Aws::String m_token;
  bool m_initialized;
  bool ExpiresSoon() const;
};
}  // namespace tiledb::sm::filesystem::s3
