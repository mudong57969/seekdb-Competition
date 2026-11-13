/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include "src/sql/engine/basic/file_scan/file_parquet_reader.h"

#include <csignal>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>

using namespace oceanbase::sql::file_scan;
namespace
{
volatile std::sig_atomic_t cancelled = 0;
void interrupt(int) { cancelled = 1; }

void print_text(std::string_view text)
{
  for (char ch : text) {
    switch (ch) {
      case '\t': std::cout << "\\t"; break;
      case '\r': std::cout << "\\r"; break;
      case '\n': std::cout << "\\n"; break;
      case '\\': std::cout << "\\\\"; break;
      case '\0': std::cout << "\\0"; break;
      default: std::cout << ch; break;
    }
  }
}
} // namespace

int main(int argc, char **argv)
{
  const bool trace = argc == 3 && std::string_view(argv[2]) == "--page-boundaries";
  if (argc != 2 && !trace) {
    std::cerr << "usage: read_parquet FILE [--page-boundaries]\n";
    return 2;
  }
  std::signal(SIGINT, interrupt);
  std::signal(SIGTERM, interrupt);
  const int fd = open(argv[1], O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC);
  if (fd < 0) {
    std::cerr << "cannot open input\n";
    return 1;
  }
  ParquetReader reader;
  const auto keep_running = [](void *) { return !cancelled; };
  const auto show_page = [](std::string_view column, int64_t row) {
    std::cerr << "PAGE\t" << column << '\t' << row << '\n';
  };
  std::vector<Column> columns;
  bool ok = reader.open(fd, keep_running, nullptr, trace ? +show_page : nullptr) && reader.infer_schema(columns);
  if (ok) {
    for (std::size_t i = 0; i < columns.size(); ++i) {
      if (i != 0) {
        std::cout << '\t';
      }
      print_text(columns[i].name);
    }
    std::cout << '\n';
    Row row;
    while ((ok = reader.next_row(row, keep_running)) && !row.empty()) {
      for (std::size_t i = 0; i < row.size(); ++i) {
        if (i != 0) {
          std::cout << '\t';
        }
        if (row[i].is_null) {
          std::cout << "NULL";
        } else {
          if (row[i].json_string && row[i].value == "NULL") {
            std::cout << '\\';
          }
          print_text(row[i].value);
        }
      }
      std::cout << '\n';
    }
  }
  if (!ok) {
    std::cerr << reader.error() << '\n';
  }
  reader.close();
  close(fd);
  return ok && std::cout ? 0 : 1;
}
