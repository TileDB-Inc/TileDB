/**
 * @file  help_command.cc
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
 * This file defines the help command.
 */

#include <iostream>

#include "commands/help_command.h"

namespace tiledb {
namespace cli {

clipp::group HelpCommand::get_cli() {
  using namespace clipp;
  auto cli = group{value("command", command_) % "Command to display help for."};
  return cli;
}

void HelpCommand::run(const std::map<std::string, clipp::group>& group_help) {
  auto cli_format = clipp::doc_formatting{}.start_column(4).doc_column(30);
  std::string description;
  if (command_ == "help") {
    description = "Displays help about a specific command.";
  } else if (command_ == "info") {
    description = "Displays information about a TileDB array.";
  } else if (command_ == "all") {
    description =
        "Command-line interface for performing common TileDB tasks. Choose a "
        "command:";
    std::cout << description << std::endl
              << std::endl
              << clipp::usage_lines(group_help.at("all"), "tiledb", cli_format)
              << std::endl;
    return;
  } else {
    std::cerr << "Unknown command " << command_ << std::endl;
    return;
  }

  std::cout << make_man_page(group_help.at(command_), "tiledb", cli_format)
                   .prepend_section("DESCRIPTION", description)
            << std::endl;
}

void HelpCommand::set_command(const std::string& command) {
  command_ = command;
}

}  // namespace cli
}  // namespace tiledb