

  /**
 * Returns the absolute posix (string) path of the input in the
 * form "file://<absolute path>"
 */
static std::string abs_path(const std::string& path);

/**
 * Returns the directory where the program is executed.
 *
 * @return The directory path where the program is executed. If the program
 * cannot retrieve the current working directory, the empty string is
 * returned.
 */
static std::string current_dir();


  static void adjacent_slashes_dedup(std::string* path);

static bool both_slashes(char a, char b);

// Internal logic for 'abs_path()'.
static std::string abs_path_internal(const std::string& path);

/**
 * It takes as input an **absolute** path, and returns it in its canonicalized
 * form, after appropriately replacing "./" and "../" in the path.
 *
 * @param path The input path passed by reference, which will be modified
 *     by the function to hold the canonicalized absolute path. Note that the
 *     path must be absolute, otherwise the function fails. In case of error
 *     (e.g., if "../" are not properly used in *path*, or if *path* is not
 *     absolute), the function sets the empty string (i.e., "") to *path*.
 * @return void
 */
static void purge_dots_from_path(std::string* path);
