/**
 * @file   GeneralHTTPCredentialsProvider.h
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
 * Aws::Auth::GeneralHTTPCredentialsProvider class, updated to support
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

namespace tiledb::sm::filesystem::s3 {
/**
 * General HTTP Credentials Provider (previously known as ECS credentials
 * provider) implementation that loads credentials from an arbitrary HTTP(S)
 * endpoint specified by the environment or loop back / Amazon ECS / Amazon EKS
 * container host metadata services by default.
 */
class GeneralHTTPCredentialsProvider
    : public Aws::Auth::AWSCredentialsProvider {
 private:
  static bool ShouldCreateGeneralHTTPProvider(
      const Aws::String& relativeUri,
      const Aws::String& absoluteUri,
      const Aws::String authToken);

 public:
  using ShouldCreateFunc = std::function<bool(
      const Aws::String& relativeUri,
      const Aws::String& absoluteUri,
      const Aws::String authToken)>;

  /**
   * Initializes the provider to retrieve credentials from a general http
   * provided endpoint every 5 minutes or before it expires.
   * @param clientConfig The client configuration to use when performing
   * requests.
   * @param relativeUri A path appended to the metadata service endpoint. OR
   * @param absoluteUri The full URI to resolve to get credentials.
   * @param authTokenFilePath A path to a file with optional authorization token
   * passed to the URI via the 'Authorization' HTTP header.
   * @param authToken An optional authorization token passed to the URI via the
   * 'Authorization' HTTP header.
   * @param refreshRateMs The number of milliseconds after which the credentials
   * will be fetched again.
   * @param ShouldCreateFunc
   */
  GeneralHTTPCredentialsProvider(
      const Aws::Client::ClientConfiguration& clientConfig,
      const Aws::String& relativeUri,
      const Aws::String& absoluteUri,
      const Aws::String& authTokenFilePath = "",
      const Aws::String& authToken = "",
      long refreshRateMs = Aws::Auth::REFRESH_THRESHOLD,
      ShouldCreateFunc shouldCreateFunc = ShouldCreateGeneralHTTPProvider);

  /**
   * Initializes the provider to retrieve credentials from a general http
   * provided endpoint every 5 minutes or before it expires.
   * @param relativeUri A path appended to the metadata service endpoint. OR
   * @param absoluteUri The full URI to resolve to get credentials.
   * @param authTokenFilePath A path to a file with optional authorization token
   * passed to the URI via the 'Authorization' HTTP header.
   * @param authToken An optional authorization token passed to the URI via the
   * 'Authorization' HTTP header.
   * @param refreshRateMs The number of milliseconds after which the credentials
   * will be fetched again.
   * @param ShouldCreateFunc
   */
  GeneralHTTPCredentialsProvider(
      const Aws::String& relativeUri,
      const Aws::String& absoluteUri,
      const Aws::String& authTokenFilePath = "",
      const Aws::String& authToken = "",
      long refreshRateMs = Aws::Auth::REFRESH_THRESHOLD,
      ShouldCreateFunc shouldCreateFunc = ShouldCreateGeneralHTTPProvider);

  /**
   * Check if GeneralHTTPCredentialsProvider was initialized with allowed
   * configuration
   * @return true if provider configuration is valid
   */
  bool IsValid() const {
    if (!m_ecsCredentialsClient)
      return false;
    if (!m_authTokenFilePath.empty())
      return !LoadTokenFromFile().empty();
    return true;
  }

  /**
   * Initializes the provider to retrieve credentials from the ECS metadata
   * service every 5 minutes, or before it expires.
   * @param resourcePath A path appended to the metadata service endpoint.
   * @param refreshRateMs The number of milliseconds after which the credentials
   * will be fetched again.
   */
  // TODO: 1.12: AWS_DEPRECATED("This c-tor is deprecated, please use one
  // above.")
  GeneralHTTPCredentialsProvider(
      const char* resourcePath,
      long refreshRateMs = Aws::Auth::REFRESH_THRESHOLD)
      : GeneralHTTPCredentialsProvider(
            resourcePath, "", "", "", refreshRateMs) {
  }

  /**
   * Initializes the provider to retrieve credentials from a provided endpoint
   * every 5 minutes or before it expires.
   * @param endpoint The full URI to resolve to get credentials.
   * @param token An optional authorization token passed to the URI via the
   * 'Authorization' HTTP header.
   * @param refreshRateMs The number of milliseconds after which the credentials
   * will be fetched again.
   */
  // TODO: 1.12: AWS_DEPRECATED("This c-tor is deprecated, please use one
  // above.")
  GeneralHTTPCredentialsProvider(
      const char* endpoint,
      const char* token,
      long refreshRateMs = Aws::Auth::REFRESH_THRESHOLD)
      : GeneralHTTPCredentialsProvider("", endpoint, token, "", refreshRateMs) {
  }

  /**
   * Initializes the provider to retrieve credentials using the provided client.
   * @param client The ECSCredentialsClient instance to use when retrieving
   * credentials.
   * @param refreshRateMs The number of milliseconds after which the credentials
   * will be fetched again.
   */
  GeneralHTTPCredentialsProvider(
      const std::shared_ptr<Aws::Internal::ECSCredentialsClient>& client,
      long refreshRateMs = Aws::Auth::REFRESH_THRESHOLD);
  /**
   * Retrieves the credentials if found, otherwise returns empty credential set.
   */
  Aws::Auth::AWSCredentials GetAWSCredentials() override;

  static const char AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE[];
  static const char AWS_CONTAINER_CREDENTIALS_RELATIVE_URI[];
  static const char AWS_CONTAINER_CREDENTIALS_FULL_URI[];
  static const char AWS_CONTAINER_AUTHORIZATION_TOKEN[];

  static const char AWS_ECS_CONTAINER_HOST[];
  static const char AWS_EKS_CONTAINER_HOST[];
  static const char AWS_EKS_CONTAINER_HOST_IPV6[];

 protected:
  void Reload() override;

 private:
  bool ExpiresSoon() const;
  void RefreshIfExpired();

  Aws::String LoadTokenFromFile() const;

  std::shared_ptr<Aws::Internal::ECSCredentialsClient> m_ecsCredentialsClient;
  Aws::String m_authTokenFilePath;

  long m_loadFrequencyMs = Aws::Auth::REFRESH_THRESHOLD;
  Aws::Auth::AWSCredentials m_credentials;
};

// GeneralHTTPCredentialsProvider was previously known as
// TaskRoleCredentialsProvider or "ECS credentials provider"
using TaskRoleCredentialsProvider = GeneralHTTPCredentialsProvider;
}  // namespace tiledb::sm::filesystem::s3
