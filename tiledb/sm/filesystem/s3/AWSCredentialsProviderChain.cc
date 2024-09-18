/**
 * @file   AWSCredentialsProviderChain.cc
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

#include "tiledb/sm/filesystem/s3/AWSCredentialsProviderChain.h"

#include "tiledb/common/common.h"
#include "tiledb/sm/filesystem/s3/GeneralHTTPCredentialsProvider.h"
#include "tiledb/sm/filesystem/s3/STSCredentialsProvider.h"

#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/SSOCredentialsProvider.h>
#include <aws/core/config/EC2InstanceProfileConfigLoader.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/memory/AWSMemory.h>

using namespace Aws::Auth;
using namespace Aws::Utils::Threading;

namespace tiledb::sm::filesystem::s3 {
static const char AWS_EC2_METADATA_DISABLED[] = "AWS_EC2_METADATA_DISABLED";
static const char DefaultCredentialsProviderChainTag[] =
    "DefaultAWSCredentialsProviderChain";

DefaultAWSCredentialsProviderChain::DefaultAWSCredentialsProviderChain(
    std::shared_ptr<const Aws::Client::ClientConfiguration> clientConfig)
    : AWSCredentialsProviderChain() {
  AddProvider(make_shared<EnvironmentAWSCredentialsProvider>(HERE()));
  AddProvider(make_shared<ProfileConfigFileAWSCredentialsProvider>(HERE()));
  AddProvider(make_shared<ProcessCredentialsProvider>(HERE()));
  // The vendored credentials providers in tiledb::sm::filesystem::s3 will be
  // picked over the built-in ones.
  AddProvider(make_shared<STSAssumeRoleWebIdentityCredentialsProvider>(
      HERE(),
      clientConfig ? *clientConfig : Aws::Client::ClientConfiguration()));
  // SSOCredentialsProvider is a complex provider and patching it to support
  // ClientConfiguration would require vendoring several files. Since support is
  // going to be added upstream soon with
  // https://github.com/aws/aws-sdk-cpp/pull/2860, let's not update it for now.
  AddProvider(make_shared<SSOCredentialsProvider>(HERE()));

  // General HTTP Credentials (prev. known as ECS TaskRole credentials) only
  // available when ENVIRONMENT VARIABLE is set
  const auto relativeUri = Aws::Environment::GetEnv(
      GeneralHTTPCredentialsProvider::AWS_CONTAINER_CREDENTIALS_RELATIVE_URI);
  AWS_LOGSTREAM_DEBUG(
      DefaultCredentialsProviderChainTag,
      "The environment variable value "
          << GeneralHTTPCredentialsProvider::
                 AWS_CONTAINER_CREDENTIALS_RELATIVE_URI
          << " is " << relativeUri);

  const auto absoluteUri = Aws::Environment::GetEnv(
      GeneralHTTPCredentialsProvider::AWS_CONTAINER_CREDENTIALS_FULL_URI);
  AWS_LOGSTREAM_DEBUG(
      DefaultCredentialsProviderChainTag,
      "The environment variable value "
          << GeneralHTTPCredentialsProvider::AWS_CONTAINER_CREDENTIALS_FULL_URI
          << " is " << absoluteUri);

  const auto ec2MetadataDisabled =
      Aws::Environment::GetEnv(AWS_EC2_METADATA_DISABLED);
  AWS_LOGSTREAM_DEBUG(
      DefaultCredentialsProviderChainTag,
      "The environment variable value " << AWS_EC2_METADATA_DISABLED << " is "
                                        << ec2MetadataDisabled);

  if (!relativeUri.empty() || !absoluteUri.empty()) {
    const Aws::String token = Aws::Environment::GetEnv(
        GeneralHTTPCredentialsProvider::AWS_CONTAINER_AUTHORIZATION_TOKEN);
    const Aws::String tokenPath = Aws::Environment::GetEnv(
        GeneralHTTPCredentialsProvider::AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE);

    auto genProvider =
        clientConfig ? make_shared<GeneralHTTPCredentialsProvider>(
                           HERE(),
                           *clientConfig,
                           relativeUri,
                           absoluteUri,
                           token,
                           tokenPath) :
                       make_shared<GeneralHTTPCredentialsProvider>(
                           HERE(), relativeUri, absoluteUri, token, tokenPath);
    if (genProvider && genProvider->IsValid()) {
      AddProvider(std::move(genProvider));
      auto& uri = !relativeUri.empty() ? relativeUri : absoluteUri;
      AWS_LOGSTREAM_INFO(
          DefaultCredentialsProviderChainTag,
          "Added General HTTP / ECS credentials provider with ur: ["
              << uri << "] to the provider chain with a"
              << ((token.empty() && tokenPath.empty()) ? "n empty " :
                                                         " non-empty ")
              << "authorization token.");
    } else {
      AWS_LOGSTREAM_ERROR(
          DefaultCredentialsProviderChainTag,
          "Unable to create GeneralHTTPCredentialsProvider");
    }
  } else if (
      Aws::Utils::StringUtils::ToLower(ec2MetadataDisabled.c_str()) != "true") {
    auto ec2MetadataClient = clientConfig ?
                                 make_shared<Aws::Internal::EC2MetadataClient>(
                                     HERE(), *clientConfig) :
                                 nullptr;
    AddProvider(make_shared<InstanceProfileCredentialsProvider>(
        HERE(),
        make_shared<Aws::Config::EC2InstanceProfileConfigLoader>(
            std::move(ec2MetadataClient))));
    AWS_LOGSTREAM_INFO(
        DefaultCredentialsProviderChainTag,
        "Added EC2 metadata service credentials provider to the provider "
        "chain.");
  }
}

DefaultAWSCredentialsProviderChain::DefaultAWSCredentialsProviderChain(
    const DefaultAWSCredentialsProviderChain& chain) {
  for (const auto& provider : chain.GetProviders()) {
    AddProvider(provider);
  }
}
}  // namespace tiledb::sm::filesystem::s3
