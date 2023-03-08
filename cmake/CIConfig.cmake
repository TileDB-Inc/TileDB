if ($ENV{GITHUB_ACTIONS} STREQUAL "true")
  include("${CMAKE_SOURCE_DIR}/.github/misc/github_actions_override.cmake")
endif()
