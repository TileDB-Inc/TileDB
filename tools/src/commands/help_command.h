/**
 * @file  help_command.h
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
 * This file declares the help command.
 */

#ifndef TILEDB_CLI_HELP_COMMAND_H
#define TILEDB_CLI_HELP_COMMAND_H

#include <map>
#include <string>

#include "commands/command.h"

namespace tiledb {
namespace cli {

/**
 * Command that can display help about other commands.
 */
class HelpCommand : public Command {
 public:
  /** Get the CLI for this command instance. */
  clipp::group get_cli();

  /**
   * Run this help command.
   *
   * @param group_help Mapping of command name -> clipp CLI for that command.
   */
  void run(const std::map<std::string, clipp::group>& group_help);

  /**
   * Sets the command to display help for.
   */
  void set_command(const std::string& command);

 private:
  /** Command to show help for. */
  std::string command_;
};

}  // namespace cli
}  // namespace tiledb

#endif