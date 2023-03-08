if (DEFINED ENV{GITHUB_ACTIONS})
  include("${CMAKE_SOURCE_DIR}/.github/misc/github_actions_override.cmake")
endif()
