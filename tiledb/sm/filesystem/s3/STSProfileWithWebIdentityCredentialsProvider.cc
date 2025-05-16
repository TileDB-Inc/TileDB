/**
 * @file   STSProfileWithWebIdentityCredentialsProvider.cc
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
 * This file implements the S3 Credentials Provider to support STS and Web
 * Identity.
 *
 * Based on
 * https://github.com/aws/aws-sdk-cpp/blob/main/src/aws-cpp-sdk-identity-management/source/auth/STSProfileCredentialsProvider.cpp
 */

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifdef HAVE_S3

#include "STSProfileWithWebIdentityCredentialsProvider.h"
#include <aws/core/client/SpecifiedRetryableErrorsRetryStrategy.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/UUID.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/sts/STSClient.h>
#include <aws/sts/model/AssumeRoleRequest.h>
#include <aws/sts/model/AssumeRoleWithWebIdentityRequest.h>
#include "tiledb/common/assert.h"

#include <utility>

#include <fstream>

using namespace Aws;
using namespace Aws::Auth;

constexpr char CLASS_TAG[] = "STSProfileWithWebIdentityCredentialsProvider";

STSProfileWithWebIdentityCredentialsProvider::
    STSProfileWithWebIdentityCredentialsProvider()
    : STSProfileWithWebIdentityCredentialsProvider(
          GetConfigProfileName(),
          std::chrono::minutes(60) /*duration*/,
          nullptr /*stsClientFactory*/) {
}

STSProfileWithWebIdentityCredentialsProvider::
    STSProfileWithWebIdentityCredentialsProvider(
        const Aws::String& profileName, std::chrono::minutes duration)
    : STSProfileWithWebIdentityCredentialsProvider(
          profileName, duration, nullptr /*stsClientFactory*/) {
}

STSProfileWithWebIdentityCredentialsProvider::
    STSProfileWithWebIdentityCredentialsProvider(
        const Aws::String& profileName,
        std::chrono::minutes duration,
        const std::function<std::shared_ptr<Aws::STS::STSClient>(
            const AWSCredentials&)>& stsClientFactory)
    : m_profileName(profileName)
    , m_duration(duration)
    , m_reloadFrequency(
          std::chrono::minutes(
              std::max(int64_t(5), static_cast<int64_t>(duration.count()))) -
          std::chrono::minutes(5))
    , m_stsClientFactory(stsClientFactory) {
}

AWSCredentials
STSProfileWithWebIdentityCredentialsProvider::GetAWSCredentials() {
  RefreshIfExpired();
  Utils::Threading::ReaderLockGuard guard(m_reloadLock);
  return m_credentials;
}

void STSProfileWithWebIdentityCredentialsProvider::RefreshIfExpired() {
  Utils::Threading::ReaderLockGuard guard(m_reloadLock);
  if (!IsTimeToRefresh(static_cast<long>(m_reloadFrequency.count())) ||
      !m_credentials.IsExpiredOrEmpty()) {
    return;
  }

  guard.UpgradeToWriterLock();
  if (!IsTimeToRefresh(static_cast<long>(m_reloadFrequency.count())) ||
      !m_credentials.IsExpiredOrEmpty())  // double-checked lock to avoid
                                          // refreshing twice
  {
    return;
  }

  Reload();
}

enum class ProfileState {
  Invalid,
  Static,
  Process,
  SourceProfile,
  SelfReferencing,  // special case of SourceProfile.
  RoleARNWebIdentity
};

/*
 * A valid profile can be in one of the following states. Any other state is
considered invalid.
 +---------+-----------+-----------+--------------+-----------+
|         |           |           |              |            |
| Role    | Source    |  Process  | Static       |   Web      |
| ARN     | Profile   |           | Credentials  |   Identity |
+------------------------------------------------+------------+
|         |           |           |              |            |
|  false  |  false    |  false    |  TRUE        |  false     |
|         |           |           |              |            |
|  false  |  false    |  TRUE     |  false       |  false     |
|         |           |           |              |            |
|  TRUE   |  TRUE     |  false    |  false       |  false     |
|         |           |           |              |            |
|  TRUE   |  TRUE     |  false    |  TRUE        |  false     |
|         |           |           |              |            |
|  TRUE   |  false    |  false    |  false       |  TRUE      |
+---------+-----------+-----------+--------------+------------+

*/
static ProfileState CheckProfile(
    const Aws::Config::Profile& profile, bool topLevelProfile) {
  constexpr int STATIC_CREDENTIALS = 1;
  constexpr int PROCESS_CREDENTIALS = 2;
  constexpr int SOURCE_PROFILE = 4;
  constexpr int ROLE_ARN = 8;
  constexpr int WEB_IDENTITY_TOKEN_FILE = 16;

  int state = 0;

  if (!profile.GetCredentials().IsExpiredOrEmpty()) {
    state += STATIC_CREDENTIALS;
  }

  if (!profile.GetCredentialProcess().empty()) {
    state += PROCESS_CREDENTIALS;
  }

  if (!profile.GetSourceProfile().empty()) {
    state += SOURCE_PROFILE;
  }

  if (!profile.GetRoleArn().empty()) {
    state += ROLE_ARN;
  }

  if (!profile.GetValue("web_identity_token_file").empty()) {
    state += WEB_IDENTITY_TOKEN_FILE;
  }

  if (topLevelProfile) {
    switch (state) {
      case 1:
        return ProfileState::Static;
      case 2:
        return ProfileState::Process;
      case 12:  // just source profile && role arn available
        return ProfileState::SourceProfile;
      case 13:  // static creds && source profile && role arn
        if (profile.GetName() == profile.GetSourceProfile()) {
          return ProfileState::SelfReferencing;
        }
        // source-profile over-rule static credentials in top-level profiles
        // (except when self-referencing)
        return ProfileState::SourceProfile;
      case 24:
        return ProfileState::RoleARNWebIdentity;
      default:
        // All other cases are considered malformed configuration.
        return ProfileState::Invalid;
    }
  } else {
    switch (state) {
      case 1:
        return ProfileState::Static;
      case 2:
        return ProfileState::Process;
      case 12:  // just source profile && role arn available
        return ProfileState::SourceProfile;
      case 13:  // static creds && source profile && role arn
        if (profile.GetName() == profile.GetSourceProfile()) {
          return ProfileState::SelfReferencing;
        }
        return ProfileState::Static;  // static credentials over-rule
                                      // source-profile (except when
                                      // self-referencing)
      case 24:
        return ProfileState::RoleARNWebIdentity;
      default:
        // All other cases are considered malformed configuration.
        return ProfileState::Invalid;
    }
  }
}

void STSProfileWithWebIdentityCredentialsProvider::Reload() {
  // make a copy of the profiles map to be able to set credentials on the
  // individual profiles when assuming role
  auto loadedProfiles = Aws::Config::GetCachedConfigProfiles();
  auto profileIt = loadedProfiles.find(m_profileName);

  if (profileIt == loadedProfiles.end()) {
    AWS_LOGSTREAM_ERROR(
        CLASS_TAG,
        "Profile " << m_profileName
                   << " was not found in the shared configuration file.");
    m_credentials = {};
    return;
  }

  ProfileState profileState =
      CheckProfile(profileIt->second, true /*topLevelProfile*/);

  if (profileState == ProfileState::Static) {
    m_credentials = profileIt->second.GetCredentials();
    AWSCredentialsProvider::Reload();
    return;
  }

  if (profileState == ProfileState::Process) {
    const auto& creds =
        GetCredentialsFromProcess(profileIt->second.GetCredentialProcess());
    if (!creds.IsExpiredOrEmpty()) {
      m_credentials = creds;
      AWSCredentialsProvider::Reload();
    }
    return;
  }

  if (profileState == ProfileState::Invalid) {
    AWS_LOGSTREAM_ERROR(
        CLASS_TAG,
        "Profile " << profileIt->second.GetName()
                   << " is invalid. Check its configuration.");
    m_credentials = {};
    return;
  }

  if (profileState == ProfileState::SourceProfile) {
    // A top level profile with a 'SourceProfile' state (determined by
    // CheckProfile rules) means that its static credentials will be ignored.
    // So, it's ok to clear them out here to simplify the logic in the chaining
    // loop below.
    profileIt->second.SetCredentials({});
  }

  if (profileState == ProfileState::RoleARNWebIdentity) {
    m_credentials = GetCredentialsFromWebIdentity(profileIt->second);
    return;
  }

  AWS_LOGSTREAM_INFO(
      CLASS_TAG,
      "Profile " << profileIt->second.GetName()
                 << " has a role ARN. Attempting to load its source "
                    "credentials from profile "
                 << profileIt->second.GetSourceProfile());

  Aws::Vector<Config::AWSProfileConfigLoader::ProfilesContainer::iterator>
      sourceProfiles;
  Aws::Set<Aws::String> visitedProfiles;

  auto currentProfile = profileIt;
  sourceProfiles.push_back(currentProfile);

  // build the chain (DAG)
  while (!currentProfile->second.GetSourceProfile().empty()) {
    ProfileState currentProfileState =
        CheckProfile(currentProfile->second, false /*topLevelProfile*/);
    auto currentProfileName = currentProfile->second.GetName();
    if (currentProfileState == ProfileState::Invalid) {
      AWS_LOGSTREAM_ERROR(
          CLASS_TAG,
          "Profile " << profileIt->second.GetName()
                     << " is invalid. Check its configuration.");
      m_credentials = {};
      return;
    }

    // terminate the chain as soon as we hit a profile with either static
    // credentials or credential process
    if (currentProfileState == ProfileState::Static ||
        currentProfileState == ProfileState::Process) {
      break;
    }

    if (currentProfileState == ProfileState::SelfReferencing) {
      sourceProfiles.push_back(currentProfile);
      break;
    }

    // check if we have a circular reference in the graph.
    if (visitedProfiles.find(currentProfileName) != visitedProfiles.end()) {
      // TODO: print the whole DAG for better debugging
      AWS_LOGSTREAM_ERROR(
          CLASS_TAG,
          "Profile " << currentProfileName
                     << " has a circular reference. Aborting.");
      m_credentials = {};
      return;
    }

    visitedProfiles.emplace(currentProfileName);

    const auto it =
        loadedProfiles.find(currentProfile->second.GetSourceProfile());
    if (it == loadedProfiles.end()) {
      // TODO: print the whole DAG for better debugging
      AWS_LOGSTREAM_ERROR(
          CLASS_TAG,
          "Profile " << currentProfileName << " has an invalid source profile "
                     << currentProfile->second.GetSourceProfile());
      m_credentials = {};
      return;
    }
    currentProfile = it;
    sourceProfiles.push_back(currentProfile);
  }

  // The last profile added to the stack is not checked. Check it now.
  if (!sourceProfiles.empty()) {
    if (CheckProfile(
            sourceProfiles.back()->second, false /*topLevelProfile*/) ==
        ProfileState::Invalid) {
      AWS_LOGSTREAM_ERROR(
          CLASS_TAG,
          "Profile " << sourceProfiles.back()->second.GetName()
                     << " is invalid. Check its configuration.");
      m_credentials = {};
      return;
    }
  }

  while (sourceProfiles.size() > 1) {
    const auto profile = sourceProfiles.back()->second;
    sourceProfiles.pop_back();

    // Check current profile in stack for type
    ProfileState state = CheckProfile(profile, false);

    AWSCredentials stsCreds;

    if (state == ProfileState::RoleARNWebIdentity) {
      stsCreds = GetCredentialsFromWebIdentity(profile);
    } else {
      if (profile.GetCredentialProcess().empty()) {
        passert(!profile.GetCredentials().IsEmpty());
        stsCreds = profile.GetCredentials();
      } else {
        stsCreds = GetCredentialsFromProcess(profile.GetCredentialProcess());
      }
    }

    // get the role arn from the profile at the top of the stack (which hasn't
    // been popped out yet)
    const auto arn = sourceProfiles.back()->second.GetRoleArn();
    const auto externID = sourceProfiles.back()->second.GetValue("external_id");
    const auto& assumedCreds = GetCredentialsFromSTS(stsCreds, arn, externID);
    sourceProfiles.back()->second.SetCredentials(assumedCreds);
  }

  if (!sourceProfiles.empty()) {
    passert(profileIt == sourceProfiles.back());
    passert(!profileIt->second.GetCredentials().IsEmpty());
  }

  m_credentials = profileIt->second.GetCredentials();
  AWSCredentialsProvider::Reload();
}

AWSCredentials
STSProfileWithWebIdentityCredentialsProvider::GetCredentialsFromSTSInternal(
    const Aws::String& roleArn,
    const Aws::String& externalID,
    Aws::STS::STSClient* client) {
  using namespace Aws::STS::Model;
  AssumeRoleRequest assumeRoleRequest;
  assumeRoleRequest.WithRoleArn(roleArn)
      .WithRoleSessionName(Aws::Utils::UUID::RandomUUID())
      .WithDurationSeconds(
          static_cast<int>(std::chrono::seconds(m_duration).count()));

  if (!externalID.empty()) {
    assumeRoleRequest.WithExternalId(externalID);
  }

  auto outcome = client->AssumeRole(assumeRoleRequest);
  if (outcome.IsSuccess()) {
    const auto& modelCredentials = outcome.GetResult().GetCredentials();
    return {
        modelCredentials.GetAccessKeyId(),
        modelCredentials.GetSecretAccessKey(),
        modelCredentials.GetSessionToken(),
        modelCredentials.GetExpiration()};
  } else {
    AWS_LOGSTREAM_ERROR(CLASS_TAG, "Failed to assume role " << roleArn);
  }
  return {};
}

AWSCredentials
STSProfileWithWebIdentityCredentialsProvider::GetCredentialsFromSTS(
    const AWSCredentials& credentials,
    const Aws::String& roleArn,
    const Aws::String& externalID) {
  using namespace Aws::STS::Model;
  if (m_stsClientFactory) {
    auto client = m_stsClientFactory(credentials);
    return GetCredentialsFromSTSInternal(roleArn, externalID, client.get());
  }

  Aws::STS::STSClient stsClient{credentials};
  return GetCredentialsFromSTSInternal(roleArn, externalID, &stsClient);
}

AWSCredentials STSProfileWithWebIdentityCredentialsProvider::
    GetCredentialsFromWebIdentityInternal(
        const Config::Profile& profile, Aws::STS::STSClient* client) {
  using namespace Aws::STS::Model;
  const Aws::String& m_roleArn = profile.GetRoleArn();
  Aws::String m_tokenFile = profile.GetValue("web_identity_token_file");
  Aws::String m_sessionName = profile.GetValue("role_session_name");

  if (m_sessionName.empty()) {
    m_sessionName = Aws::Utils::UUID::RandomUUID();
  }

  Aws::IFStream tokenFile(m_tokenFile.c_str());
  Aws::String m_token;
  if (tokenFile) {
    Aws::String token(
        (std::istreambuf_iterator<char>(tokenFile)),
        std::istreambuf_iterator<char>());
    m_token = token;
  } else {
    AWS_LOGSTREAM_ERROR(CLASS_TAG, "Can't open token file: " << m_tokenFile);
    return {};
  }

  AssumeRoleWithWebIdentityRequest request;
  request.SetRoleArn(m_roleArn);
  request.SetRoleSessionName(m_sessionName);
  request.SetWebIdentityToken(m_token);

  auto outcome = client->AssumeRoleWithWebIdentity(request);
  if (outcome.IsSuccess()) {
    const auto& modelCredentials = outcome.GetResult().GetCredentials();
    AWS_LOGSTREAM_TRACE(
        CLASS_TAG,
        "Successfully retrieved credentials with AWS_ACCESS_KEY: "
            << modelCredentials.GetAccessKeyId());
    return {
        modelCredentials.GetAccessKeyId(),
        modelCredentials.GetSecretAccessKey(),
        modelCredentials.GetSessionToken(),
        modelCredentials.GetExpiration()};
  } else {
    AWS_LOGSTREAM_ERROR(CLASS_TAG, "failed to assume role" << m_roleArn);
  }
  return {};
}

AWSCredentials
STSProfileWithWebIdentityCredentialsProvider::GetCredentialsFromWebIdentity(
    const Config::Profile& profile) {
  using namespace Aws::STS::Model;
  if (m_stsClientFactory) {
    auto client = m_stsClientFactory({});
    return GetCredentialsFromWebIdentityInternal(profile, client.get());
  }

  Aws::STS::STSClient stsClient{AWSCredentials{}};
  return GetCredentialsFromWebIdentityInternal(profile, &stsClient);
}

#endif  // HAVE_S3s
