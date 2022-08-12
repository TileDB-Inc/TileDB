# ~~~
# Copyright 2020 Google LLC
# Copyright 2022 TileDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ~~~

message("PATCHING absl/base/options.h")

file(READ "absl/base/options.h" content)
# Using REGEX REPLACE does not work for some reason. We replace each macro and
# verify (at the end) that there are no other such macros left
string(REPLACE "#define ABSL_OPTION_USE_STD_ANY 2"
               "#define ABSL_OPTION_USE_STD_ANY 0" content "${content}")
string(REPLACE "#define ABSL_OPTION_USE_STD_OPTIONAL 2"
               "#define ABSL_OPTION_USE_STD_OPTIONAL 0" content "${content}")
string(REPLACE "#define ABSL_OPTION_USE_STD_STRING_VIEW 2"
               "#define ABSL_OPTION_USE_STD_STRING_VIEW 0" content "${content}")
string(REPLACE "#define ABSL_OPTION_USE_STD_VARIANT 2"
               "#define ABSL_OPTION_USE_STD_VARIANT 0" content "${content}")

string(REGEX MATCH "#define ABSL_OPTION_USE_.* 2\n" matches "${content}")
if ("${matches}" STREQUAL "")
    file(WRITE "absl/base/options.h" "${content}")
else ()
    file(
        WRITE "absl/base/options.h"
        "#error \"tiledb cannot fully patch this file, fix abseil-patch.cmake\"\n"
    )
    message(ERROR_FATAL "##### <${matches}>")
endif ()
