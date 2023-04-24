# -----------------------------------------------------------------------------
# Allow our executables to use Catch2.
# -----------------------------------------------------------------------------
include(FetchContent)

# find_package(Catch2 3 REQUIRED)

FetchContent_Declare(
  catch2
  GIT_REPOSITORY "https://github.com/catchorg/Catch2.git"
  GIT_TAG        "v3.1.0"
)

FetchContent_MakeAvailable(catch2)
