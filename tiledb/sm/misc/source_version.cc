#include <string.h>
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb_git_sha.h"

extern "C" {

void tiledb_source_version(char* version_string) {
  const char* _sha1 = _tiledb_str(TILEDB_GIT_HASH);
  strncpy(version_string, _sha1, 40);
};
}
