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

#include <cstdio>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <utility>

namespace oceanbase::sql::file_scan
{

enum class Format { Csv, Ndjson, Json, Parquet, Glob };

Format format_from_identity(std::string_view identity);

enum class Type { Null, Integer, Double, Boolean, Date, DateTime, Text, Decimal };

struct Scalar {
  std::int64_t integer = 0;
  double real = 0;
};

// DATE uses days since 1970-01-01 and DATETIME uses microseconds since then.
bool parse_scalar(Type type, std::string_view input, Scalar &value);

struct Column {
  std::string name;
  Type type = Type::Null;
  bool nullable = false;
  bool double_compatible = true;
  int64_t precision = -1;
  int64_t scale = -1;
};

struct Field {
  std::string value;
  bool is_null = false;
  bool json_string = false;
};

using Row = std::vector<Field>;

inline constexpr char CSV_COPY_MODE[] = "local_csv_copy";

class CsvExport {
public:
  CsvExport() = default;
  ~CsvExport();
  CsvExport(const CsvExport &) = delete;
  CsvExport &operator=(const CsvExport &) = delete;

  int open(std::string allowed, const std::string &path,
           const std::vector<std::string> &protected_directories);
  int write_field(std::string_view value, bool quoted);
  int write_separator(char ch);
  int flush();
  int publish(std::string allowed);

private:
  int open_parent(const std::string &allowed) const;
  std::FILE *file_ = nullptr;
  int parent_ = -1;
  std::string directory_;
  std::string name_;
};

class ParquetReader;

class CsvReader {
public:
  CsvReader();
  ~CsvReader();
  CsvReader(const CsvReader &) = delete;
  CsvReader &operator=(const CsvReader &) = delete;

  using Continue = bool (*)(void *);
  bool open(bool enabled, const std::string &allowed_directory, const std::string &path,
            Format format = Format::Csv, Continue should_continue = nullptr, void *context = nullptr);
  // Scan all records and position the stream immediately after the header.
  bool infer(std::vector<Column> &columns, Continue should_continue = nullptr, void *context = nullptr);
  bool start_scan(const std::string &expected_identity, std::size_t width,
                  const std::vector<std::string> &names = {},
                  Continue should_continue = nullptr, void *context = nullptr);
  bool rescan();
  // An empty batch indicates EOF. On error the batch is discarded.
  bool next_batch(std::size_t max_rows, std::vector<Row> &rows,
                  Continue should_continue = nullptr, void *context = nullptr);
  const std::string &identity() const { return identity_; }
  const std::string &error() const { return error_; }

private:
  enum class Result { Row, Eof, Error };
  Result read_record(Row &row);
  using Object = std::vector<std::pair<std::string, Field>>;
  Result read_object(Object &object, Continue should_continue, void *context);
  bool parse_object(const std::string &record, Object &object);
  Result read_array_object(Object &object, Continue should_continue, void *context);
  bool infer_json_objects(std::vector<Column> &columns, Continue should_continue, void *context);
  bool set_json_schema(const std::vector<std::string> &names);
  Result read_json_row(Row &row, Continue should_continue, void *context);
  struct GlobFile {
    std::string name;
    std::string identity;
  };
  bool prepare_parquet(std::vector<Column> &columns, Continue should_continue, void *context);
  bool open_single(const std::string &allowed, const std::string &path);
  bool open_glob(const std::string &allowed, const std::string &path,
                 Continue should_continue, void *context);
  bool open_glob_member(const GlobFile &file, CsvReader &reader);
  bool infer_glob(std::vector<Column> &columns, Continue should_continue, void *context);
  bool start_glob_member();
  bool next_glob_batch(std::size_t max_rows, std::vector<Row> &rows,
                       Continue should_continue, void *context);
  bool fail(const char *message);
  bool check_identity();
  std::string current_identity() const;
  void close();

  enum class ArrayState { Start, First, AfterObject, Done };
  Format format_ = Format::Csv;
  std::unique_ptr<ParquetReader> parquet_;
  std::vector<Column> parquet_columns_;
  bool glob_member_ = false;
  std::string glob_pattern_;
  std::string glob_directory_;
  std::string glob_allowed_;
  std::vector<GlobFile> glob_files_;
  std::vector<std::string> glob_names_;
  std::vector<std::size_t> glob_mapping_;
  std::unique_ptr<CsvReader> glob_reader_;
  std::size_t glob_index_ = 0;
  Row csv_header_;
  ArrayState array_state_ = ArrayState::Start;
  std::unordered_map<std::string, std::size_t> json_indexes_;
  std::FILE *file_ = nullptr;
  long data_offset_ = -1;
  std::size_t width_ = 0;
  std::string identity_;
  std::string error_;
};

} // namespace oceanbase::sql::file_scan
