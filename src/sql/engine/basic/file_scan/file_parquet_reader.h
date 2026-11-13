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


#pragma once

#include "file_csv_reader.h"

namespace oceanbase::sql::file_scan
{

// The caller owns the regular descriptor and must enforce the SQL path policy
// before passing it here. The decoder never reopens a pathname.
class ParquetReader {
public:
  using PageCallback = void (*)(std::string_view column, int64_t row);
  ParquetReader();
  ~ParquetReader();
  ParquetReader(const ParquetReader &) = delete;
  ParquetReader &operator=(const ParquetReader &) = delete;

  bool open(int fd, CsvReader::Continue should_continue = nullptr, void *context = nullptr,
            PageCallback trace = nullptr);
  bool infer_schema(std::vector<Column> &columns);
  // A successful empty row indicates EOF.
  bool next_row(Row &row, CsvReader::Continue should_continue = nullptr, void *context = nullptr);
  bool rescan();
  void close();
  const std::string &error() const { return error_; }

private:
  struct State;
  std::unique_ptr<State> state_;
  std::string error_;
};

} // namespace oceanbase::sql::file_scan
