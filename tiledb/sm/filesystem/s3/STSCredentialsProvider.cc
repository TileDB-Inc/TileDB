/**
 * @file   STSCredentialsProvider.cc
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

#ifdef HAVE_S3

#include "tiledb/sm/filesystem/s3/STSCredentialsProvider.h"
#include "tiledb/common/common.h"

#include <aws/core/client/SpecifiedRetryableErrorsRetryStrategy.h>
#include <aws/core/config/AWSProfileConfigLoader.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/platform/FileSystem.h>
#include <aws/core/utils/FileSystemUtils.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/UUID.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <string.h>
#include <climits>
#include <cstdlib>
#include <fstream>

using namespace Aws::Utils;
using namespace Aws::Utils::Logging;
using namespace Aws::Auth;
using namespace Aws::Internal;
using namespace Aws::FileSystem;
using namespace Aws::Client;
using Aws::Utils::Threading::ReaderLockGuard;
using Aws::Utils::Threading::WriterLockGuard;

static const char STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG[] =
    "STSAssumeRoleWithWebIdentityCredentialsProvider";
static const int STS_CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD = 5 * 1000;

namespace tiledb::sm::filesystem::s3 {
STSAssumeRoleWebIdentityCredentialsProvider::
    STSAssumeRoleWebIdentityCredentialsProvider(
        Aws::Client::ClientConfiguration config)
    : m_initialized(false) {
  // check environment variables
  Aws::String tmpRegion = Aws::Environment::GetEnv("AWS_DEFAULT_REGION");
  m_roleArn = Aws::Environment::GetEnv("AWS_ROLE_ARN");
  m_tokenFile = Aws::Environment::GetEnv("AWS_WEB_IDENTITY_TOKEN_FILE");
  m_sessionName = Aws::Environment::GetEnv("AWS_ROLE_SESSION_NAME");

  // check profile_config if either m_roleArn or m_tokenFile is not loaded from
  // environment variable region source is not enforced, but we need it to
  // construct sts endpoint, if we can't find from environment, we should check
  // if it's set in config file.
  if (m_roleArn.empty() || m_tokenFile.empty() || tmpRegion.empty()) {
    auto profile =
        Aws::Config::GetCachedConfigProfile(Aws::Auth::GetConfigProfileName());
    if (tmpRegion.empty()) {
      tmpRegion = profile.GetRegion();
    }
    // If either of these two were not found from environment, use whatever
    // found for all three in config file
    if (m_roleArn.empty() || m_tokenFile.empty()) {
      m_roleArn = profile.GetRoleArn();
      m_tokenFile = profile.GetValue("web_identity_token_file");
      m_sessionName = profile.GetValue("role_session_name");
    }
  }

  if (m_tokenFile.empty()) {
    AWS_LOGSTREAM_WARN(
        STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
        "Token file must be specified to use STS AssumeRole web identity creds "
        "provider.");
    return;  // No need to do further constructing
  } else {
    AWS_LOGSTREAM_DEBUG(
        STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
        "Resolved token_file from profile_config or environment variable to be "
            << m_tokenFile);
  }

  if (m_roleArn.empty()) {
    AWS_LOGSTREAM_WARN(
        STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
        "RoleArn must be specified to use STS AssumeRole web identity creds "
        "provider.");
    return;  // No need to do further constructing
  } else {
    AWS_LOGSTREAM_DEBUG(
        STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
        "Resolved role_arn from profile_config or environment variable to be "
            << m_roleArn);
  }

  if (tmpRegion.empty()) {
    tmpRegion = Aws::Region::US_EAST_1;
  } else {
    AWS_LOGSTREAM_DEBUG(
        STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
        "Resolved region from profile_config or environment variable to be "
            << tmpRegion);
  }

  if (m_sessionName.empty()) {
    m_sessionName = Aws::Utils::UUID::PseudoRandomUUID();
  } else {
    AWS_LOGSTREAM_DEBUG(
        STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
        "Resolved session_name from profile_config or environment variable to "
        "be "
            << m_sessionName);
  }

  config.scheme = Aws::Http::Scheme::HTTPS;
  config.region = tmpRegion;

  if (!config.retryStrategy) {
    Aws::Vector<Aws::String> retryableErrors;
    retryableErrors.push_back("IDPCommunicationError");
    retryableErrors.push_back("InvalidIdentityToken");

    config.retryStrategy = make_shared<SpecifiedRetryableErrorsRetryStrategy>(
        HERE(), retryableErrors, 3 /*maxRetries*/);
  }

  m_client = tdb_unique_ptr<Aws::Internal::STSCredentialsClient>(
      tdb_new(Aws::Internal::STSCredentialsClient, config));
  m_initialized = true;
  AWS_LOGSTREAM_INFO(
      STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
      "Creating STS AssumeRole with web identity creds provider.");
}

AWSCredentials
STSAssumeRoleWebIdentityCredentialsProvider::GetAWSCredentials() {
  // A valid client means required information like role arn and token file were
  // constructed correctly. We can use this provider to load creds, otherwise,
  // we can just return empty creds.
  if (!m_initialized) {
    return Aws::Auth::AWSCredentials();
  }
  RefreshIfExpired();
  ReaderLockGuard guard(m_reloadLock);
  return m_credentials;
}

void STSAssumeRoleWebIdentityCredentialsProvider::Reload() {
  AWS_LOGSTREAM_INFO(
      STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
      "Credentials have expired, attempting to renew from STS.");

  Aws::IFStream tokenFile(m_tokenFile.c_str());
  if (tokenFile) {
    Aws::String token(
        (std::istreambuf_iterator<char>(tokenFile)),
        std::istreambuf_iterator<char>());
    m_token = token;
  } else {
    AWS_LOGSTREAM_ERROR(
        STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
        "Can't open token file: " << m_tokenFile);
    return;
  }
  STSCredentialsClient::STSAssumeRoleWithWebIdentityRequest request{
      m_sessionName, m_roleArn, m_token};

  auto result = m_client->GetAssumeRoleWithWebIdentityCredentials(request);
  AWS_LOGSTREAM_TRACE(
      STS_ASSUME_ROLE_WEB_IDENTITY_LOG_TAG,
      "Successfully retrieved credentials with AWS_ACCESS_KEY: "
          << result.creds.GetAWSAccessKeyId());
  m_credentials = result.creds;
}

bool STSAssumeRoleWebIdentityCredentialsProvider::ExpiresSoon() const {
  return (
      (m_credentials.GetExpiration() - Aws::Utils::DateTime::Now()).count() <
      STS_CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD);
}

void STSAssumeRoleWebIdentityCredentialsProvider::RefreshIfExpired() {
  ReaderLockGuard guard(m_reloadLock);
  if (!m_credentials.IsEmpty() && !ExpiresSoon()) {
    return;
  }

  guard.UpgradeToWriterLock();
  if (!m_credentials.IsExpiredOrEmpty() &&
      !ExpiresSoon())  // double-checked lock to avoid refreshing twice
  {
    return;
  }

  Reload();
}
}  // namespace tiledb::sm::filesystem::s3

#endif
