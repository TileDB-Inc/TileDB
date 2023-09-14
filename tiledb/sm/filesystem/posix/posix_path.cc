bool Posix::both_slashes(char a, char b) {
  return a == '/' && b == '/';
}




void Posix::adjacent_slashes_dedup(std::string* path) {
  assert(utils::parse::starts_with(*path, "file://"));
  path->erase(
      std::unique(
          path->begin() + std::string("file://").size(),
          path->end(),
          both_slashes),
      path->end());
}

std::string Posix::abs_path(const std::string& path) {
  std::string resolved_path = abs_path_internal(path);

  // Ensure the returned has the same postfix slash as 'path'.
  if (utils::parse::ends_with(path, "/")) {
    if (!utils::parse::ends_with(resolved_path, "/")) {
      resolved_path = resolved_path + "/";
    }
  } else {
    if (utils::parse::ends_with(resolved_path, "/")) {
      resolved_path = resolved_path.substr(0, resolved_path.length() - 1);
    }
  }

  return resolved_path;
}

std::string Posix::abs_path_internal(const std::string& path) {
  // Initialize current, home and root
  std::string current = current_dir();
  auto env_home_ptr = getenv("HOME");
  std::string home = env_home_ptr != nullptr ? env_home_ptr : current;
  std::string root = "/";
  std::string posix_prefix = "file://";

  // Easy cases
  if (path.empty() || path == "." || path == "./")
    return posix_prefix + current;
  if (path == "~")
    return posix_prefix + home;
  if (path == "/")
    return posix_prefix + root;

  // Other cases
  std::string ret_dir;
  if (utils::parse::starts_with(path, posix_prefix))
    return path;
  else if (utils::parse::starts_with(path, "/"))
    ret_dir = posix_prefix + path;
  else if (utils::parse::starts_with(path, "~/"))
    ret_dir = posix_prefix + home + path.substr(1, path.size() - 1);
  else if (utils::parse::starts_with(path, "./"))
    ret_dir = posix_prefix + current + path.substr(1, path.size() - 1);
  else
    ret_dir = posix_prefix + current + "/" + path;

  adjacent_slashes_dedup(&ret_dir);
  purge_dots_from_path(&ret_dir);

  return ret_dir;
}

std::string Posix::current_dir() {
  static std::unique_ptr<char, decltype(&free)> cwd_(getcwd(nullptr, 0), free);
  std::string dir = cwd_.get();
  return dir;
}

void Posix::purge_dots_from_path(std::string* path) {
  // Trivial case
  if (path == nullptr)
    return;

  // Trivial case
  uint64_t path_size = path->size();
  if (path_size == 0 || *path == "file:///")
    return;

  assert(utils::parse::starts_with(*path, "file:///"));

  // Tokenize
  const char* token_c_str = path->c_str() + 8;
  std::vector<std::string> tokens, final_tokens;
  std::string token;

  for (uint64_t i = 8; i < path_size; ++i) {
    if ((*path)[i] == '/') {
      (*path)[i] = '\0';
      token = token_c_str;
      if (!token.empty())
        tokens.push_back(token);
      token_c_str = path->c_str() + i + 1;
    }
  }
  token = token_c_str;
  if (!token.empty())
    tokens.push_back(token);

  // Purge dots
  for (auto& t : tokens) {
    if (t == ".")  // Skip single dots
      continue;

    if (t == "..") {
      if (final_tokens.empty()) {
        // Invalid path
        *path = "";
        return;
      }

      final_tokens.pop_back();
    } else {
      final_tokens.push_back(t);
    }
  }

  // Assemble final path
  *path = "file://";
  for (auto& t : final_tokens)
    *path += std::string("/") + t;
}

