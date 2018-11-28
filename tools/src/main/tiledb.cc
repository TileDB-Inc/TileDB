/**
 * @file tiledb.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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
 * Command-line interface for common TileDB tasks.
 */

#include <clipp.h>
#include <iostream>
#include <map>
#include <string>

#include "commands/help_command.h"
#include "commands/info_command.h"

using namespace tiledb::cli;

int main(int argc, char** argv) {
  using namespace clipp;

  enum class Mode { Undef, Info, Help };
  Mode mode = Mode::Undef;

  InfoCommand info;
  auto info_mode = (command("info").set(mode, Mode::Info), info.get_cli());

  HelpCommand help;
  auto help_mode = (command("help").set(mode, Mode::Help), help.get_cli());

  auto cli = help_mode | info_mode;
  std::map<std::string, clipp::group> groups = {
      {"all", cli}, {"help", help_mode}, {"info", info_mode}};

  if (parse(argc, argv, cli)) {
    switch (mode) {
      case Mode::Info:
        info.run();
        break;
      case Mode::Help:
        help.run(groups);
        break;
      default:
        help.set_command("all");
        help.run(groups);
        break;
    }
  } else {
    help.set_command("all");
    help.run(groups);
    return 1;
  }

  return 0;
}