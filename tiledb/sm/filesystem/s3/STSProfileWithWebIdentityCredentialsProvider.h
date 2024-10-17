/**
 * @file   STSProfileWithWebIdentityCredentialsProvider.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file defines the S3 Credentials Provider to support STS and Web
 * Identity.
 *
 * This is based on STSProfileCredentialsProvider.h
 */

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef TILEDB_S3_STS_PROFILE_WITH_WEB_IDENTITY_CREDENTIALS_PROVIDER_H
#define TILEDB_S3_STS_PROFILE_WITH_WEB_IDENTITY_CREDENTIALS_PROVIDER_H

#ifdef HAVE_S3
#include <aws/identity-management/auth/STSProfileCredentialsProvider.h>

#include <chrono>
#include <functional>

namespace Aws {
namespace STS {
class STSClient;
}

namespace Auth {
/**
 * Credentials provider for STS Assume Role using the information in the shared
 * config file. The shared configuration file is typically created using the AWS
 * CLI and is located in: ~/.aws/config The location of the file can also be
 * controlled via environment variables. For more information see
 * https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html
 */
class /* AWS_IDENTITY_MANAGEMENT_API */
    STSProfileWithWebIdentityCredentialsProvider
    // AWS_IDENTITY_MANAGEMENT_API adds a __declspec(dllimport) on Windows
    // which causes a link error because it tried to find the implementation
    // of the class externally, but it is defined by ourselves.
    : public STSProfileCredentialsProvider {
 public:
  /**
   * Use the default profile name.
   * The default profile name can be set using environment variables; otherwise
   * it is the literal "default".
   */
  STSProfileWithWebIdentityCredentialsProvider();

  /**
   * Use the provided profile name from the shared configuration file.
   *
   * @param profileName The name of the profile in the shared configuration
   * file.
   * @param duration The duration, in minutes, of the role session, after which
   * the credentials are expired. The value can range from 15 minutes up to the
   * maximum session duration setting for the role. By default, the duration is
   * set to 1 hour. Note: This credential provider refreshes the credentials 5
   * minutes before their expiration time. That ensures the credentials do not
   * expire between the time they're checked and the time they're returned to
   * the user.
   * If the duration for the credentials is 5 minutes or less, the provider will
   * refresh the credentials only when they expire.
   *
   */
  STSProfileWithWebIdentityCredentialsProvider(
      const Aws::String& profileName,
      std::chrono::minutes duration = std::chrono::minutes(60));

  STSProfileWithWebIdentityCredentialsProvider(
      const Aws::String& profileName,
      std::chrono::minutes duration,
      const std::function<std::shared_ptr<Aws::STS::STSClient>(
          const AWSCredentials&)>& stsClientFactory);

  /**
   * Fetches the credentials set from STS following the rules defined in the
   * shared configuration file.
   */
  AWSCredentials GetAWSCredentials() override;

 protected:
  void RefreshIfExpired();
  void Reload() override;
  /**
   * Assumes a role given its ARN. Communication with STS is done through the
   * provided credentials. Returns the assumed role credentials or empty
   * credentials on error.
   */
  AWSCredentials GetCredentialsFromSTS(
      const AWSCredentials& credentials,
      const Aws::String& roleARN,
      const Aws::String& externalID);

  /**
   *  Assumes a role given a ARN and a web token
   * @param profile
   * @return
   */
  AWSCredentials GetCredentialsFromWebIdentity(const Config::Profile& profile);

 private:
  AWSCredentials GetCredentialsFromSTSInternal(
      const Aws::String& roleArn,
      const Aws::String& externalID,
      Aws::STS::STSClient* client);

  AWSCredentials GetCredentialsFromWebIdentityInternal(
      const Config::Profile& profile, Aws::STS::STSClient* client);

  Aws::String m_profileName;
  AWSCredentials m_credentials;
  const std::chrono::minutes m_duration;
  const std::chrono::milliseconds m_reloadFrequency;
  std::function<std::shared_ptr<Aws::STS::STSClient>(const AWSCredentials&)>
      m_stsClientFactory;
};
}  // namespace Auth
}  // namespace Aws
#endif  // HAVE_S3
#endif  // TILEDB_S3_STS_PROFILE_WITH_WEB_IDENTITY_CREDENTIALS_PROVIDER_H
