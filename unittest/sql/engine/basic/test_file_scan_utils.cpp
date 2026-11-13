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

#include <csignal>
#include <sys/resource.h>
#include <sys/wait.h>
#include "src/sql/engine/basic/file_scan/file_csv_reader.h"
#include <gtest/gtest.h>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sys/stat.h>

namespace fs = std::filesystem;
using namespace oceanbase::sql::file_scan;

class CsvTest : public testing::Test {
protected:
  void SetUp() override
  {
    char path[] = "/tmp/seekdb-file-csv-XXXXXX";
    const char *created = mkdtemp(path);
    ASSERT_NE(nullptr, created);
    directory = created;
  }

  void TearDown() override
  {
    if (!directory.empty()) {
      fs::remove_all(directory);
    }
  }

  void write(const std::string &content)
  {
    std::ofstream(directory / "input.csv", std::ios::binary) << content;
  }

  fs::path directory;
};

TEST_F(CsvTest, InfersToEofAndRescans)
{
  const std::string large(16000, 'x');
  write("age,name\r\n18,\"" + large + "\r\n\"\"tail\"\r\nNULL,\"NULL\"\nlate,\xe5\xbc\xa0\n");
  CsvReader reader;
  ASSERT_TRUE(reader.open(true, directory, directory / "input.csv")) << reader.error();
  std::vector<Column> columns;
  ASSERT_TRUE(reader.infer(columns)) << reader.error();
  ASSERT_EQ(2U, columns.size());
  EXPECT_EQ("age", columns[0].name);
  EXPECT_EQ(Type::Text, columns[0].type);
  EXPECT_TRUE(columns[0].nullable);

  std::vector<Row> rows;
  ASSERT_TRUE(reader.next_batch(2, rows)) << reader.error();
  ASSERT_EQ(2U, rows.size());
  EXPECT_EQ(large + "\r\n\"tail", rows[0][1].value);
  EXPECT_TRUE(rows[1][0].is_null);
  EXPECT_FALSE(rows[1][1].is_null);
  ASSERT_TRUE(reader.next_batch(2, rows));
  ASSERT_EQ(1U, rows.size());
  EXPECT_EQ("\xe5\xbc\xa0", rows[0][1].value);
  ASSERT_TRUE(reader.rescan());
  ASSERT_TRUE(reader.next_batch(1, rows));
  ASSERT_EQ(1U, rows.size());
  EXPECT_EQ("18", rows[0][0].value);
}

TEST_F(CsvTest, DeniesDisabledTraversalAndNonRegularFiles)
{
  write("a\n1\n");
  fs::create_symlink("/etc/passwd", directory / "link");
  ASSERT_EQ(0, mkfifo((directory / "pipe").c_str(), 0600));
  CsvReader reader;
  EXPECT_FALSE(reader.open(false, directory, directory / "input.csv"));
  EXPECT_FALSE(reader.open(true, directory, directory / "../etc/passwd"));
  EXPECT_FALSE(reader.open(true, directory, "/etc/passwd"));
  EXPECT_FALSE(reader.open(true, directory, directory / "link"));
  EXPECT_FALSE(reader.open(true, directory, directory / "pipe"));
}

TEST_F(CsvTest, DisambiguatesDuplicateHeadersWithoutTakingExistingNames)
{
  CsvReader reader;
  std::vector<Column> columns;
  write("id,id,id__2,ID\n1,2,3,4\n");
  ASSERT_TRUE(reader.open(true, directory, directory / "input.csv"));
  ASSERT_TRUE(reader.infer(columns)) << reader.error();
  ASSERT_EQ(4U, columns.size());
  EXPECT_EQ("id", columns[0].name);
  EXPECT_EQ("id__3", columns[1].name);
  EXPECT_EQ("id__2", columns[2].name);
  EXPECT_EQ("ID__4", columns[3].name);
}

TEST_F(CsvTest, EmptyAndMalformedFiles)
{
  CsvReader reader;
  std::vector<Column> columns;
  write("");
  ASSERT_TRUE(reader.open(true, directory, directory / "input.csv"));
  EXPECT_FALSE(reader.infer(columns));
  write("a,b\n1,2\n");
  ASSERT_TRUE(reader.open(true, directory, directory / "input.csv"));
  ASSERT_TRUE(reader.infer(columns));
  EXPECT_EQ(Type::Integer, columns[0].type);
  write("a,b\n1\n");
  ASSERT_TRUE(reader.open(true, directory, directory / "input.csv"));
  EXPECT_FALSE(reader.infer(columns));
  write("a\n\"\xff\"\n");
  ASSERT_TRUE(reader.open(true, directory, directory / "input.csv"));
  EXPECT_FALSE(reader.infer(columns));
}

TEST_F(CsvTest, RejectsReplacementAndInPlaceChanges)
{
  const std::string path = directory / "input.csv";
  write("age\n20\n21\n");
  CsvReader bound;
  std::vector<Column> columns;
  ASSERT_TRUE(bound.open(true, directory, path));
  ASSERT_TRUE(bound.infer(columns));
  const auto identity = bound.identity();

  fs::rename(path, directory / "old.csv");
  write("age\n25\n26\n");
  CsvReader execution;
  ASSERT_TRUE(execution.open(true, directory, path));
  EXPECT_NE(identity, execution.identity());
  EXPECT_FALSE(execution.start_scan(identity, 1));
  EXPECT_FALSE(execution.infer(columns, [](void *) { return false; }));
  EXPECT_TRUE(columns.empty());

  ASSERT_TRUE(execution.start_scan(execution.identity(), 1));
  write("age\n25\n26\n27\n");
  std::vector<Row> rows;
  EXPECT_FALSE(execution.next_batch(1, rows));
  EXPECT_TRUE(rows.empty());
  EXPECT_FALSE(execution.rescan());
}

TEST_F(CsvTest, InfersNativeTypesAndKeepsLossyMixesAsText)
{
  write("i,d,b,day,stamp,empty,wide,bad\n"
        "12,0.1,true,2024-02-29,2024-02-29,NULL,9007199254740993,2024-02-29\n"
        "-7,2,FaLsE,1970-01-01,2024-03-01T10:11:12.123456,,1.5,2023-02-29\n");
  CsvReader reader;
  std::vector<Column> columns;
  ASSERT_TRUE(reader.open(true, directory, directory / "input.csv"));
  ASSERT_TRUE(reader.infer(columns)) << reader.error();
  ASSERT_EQ(8U, columns.size());
  EXPECT_EQ(Type::Integer, columns[0].type);
  EXPECT_EQ(Type::Double, columns[1].type);
  EXPECT_EQ(Type::Boolean, columns[2].type);
  EXPECT_EQ(Type::Date, columns[3].type);
  EXPECT_EQ(Type::DateTime, columns[4].type);
  EXPECT_EQ(Type::Null, columns[5].type);
  EXPECT_TRUE(columns[5].nullable);
  EXPECT_EQ(Type::Text, columns[6].type);
  EXPECT_EQ(Type::Text, columns[7].type);

  Scalar value{};
  ASSERT_TRUE(parse_scalar(Type::Date, "1970-01-01", value));
  EXPECT_EQ(0, value.integer);
  ASSERT_TRUE(parse_scalar(Type::DateTime, "1970-01-02T00:00:01.5", value));
  EXPECT_EQ(86401500000LL, value.integer);
  EXPECT_FALSE(parse_scalar(Type::DateTime, "2024-02-29 23:59:60", value));
  EXPECT_FALSE(parse_scalar(Type::Date, "2023-02-29", value));
  EXPECT_FALSE(parse_scalar(Type::Double, "9007199254740993", value));
  EXPECT_FALSE(parse_scalar(Type::Double, "1.234567890123456789", value));
  EXPECT_FALSE(parse_scalar(Type::Double, "1e99999", value));
  EXPECT_FALSE(parse_scalar(Type::Double, "NaN", value));
  ASSERT_TRUE(parse_scalar(Type::Double, "01.00e-1", value));
  EXPECT_DOUBLE_EQ(0.1, value.real);
}

TEST_F(CsvTest, NdjsonStreamsLateFieldsAndRejectsBadObjects)
{
  const std::string path = directory / "input.csv";
  write(R"({"id":1,"text":")" + std::string(16000, 'x') +
        "\"}\n\n" R"({"late":true,"id":2,"text":"12"})" "\n");
  CsvReader reader;
  std::vector<Column> columns;
  ASSERT_TRUE(reader.open(true, directory, path, Format::Ndjson));
  ASSERT_TRUE(reader.infer(columns)) << reader.error();
  ASSERT_EQ(3U, columns.size());
  EXPECT_EQ("late", columns[2].name);
  EXPECT_EQ(Type::Boolean, columns[2].type);
  EXPECT_TRUE(columns[2].nullable);
  std::vector<Row> rows;
  ASSERT_TRUE(reader.next_batch(2, rows)) << reader.error();
  ASSERT_EQ(2U, rows.size());
  EXPECT_TRUE(rows[0][2].is_null);
  EXPECT_EQ("true", rows[1][2].value);
  EXPECT_EQ("2", rows[1][0].value);
  EXPECT_TRUE(reader.rescan());

  for (const char *bad : {"[1]", R"({"a":{},"b":1})",
                          R"({"a":1,"a":2})", R"({"a":1} trailing)"}) {
    write(bad);
    ASSERT_TRUE(reader.open(true, directory, path, Format::Ndjson));
    EXPECT_FALSE(reader.infer(columns)) << bad;
  }
}

TEST_F(CsvTest, StreamsJsonArrayAndRescans)
{
  const std::string path = directory / "input.csv";
  const std::string padding(16000, 'x');
  write("[\n{\"id\":1,\"text\":\"" + padding +
        R"(\"]}["}, {"late":true,"id":2,"text":null} ])");
  CsvReader reader;
  std::vector<Column> columns;
  ASSERT_TRUE(reader.open(true, directory, path, Format::Json));
  ASSERT_TRUE(reader.infer(columns)) << reader.error();
  ASSERT_EQ(3U, columns.size());
  EXPECT_EQ("late", columns[2].name);
  EXPECT_TRUE(columns[2].nullable);
  for (int pass = 0; pass < 2; ++pass) {
    std::vector<Row> rows;
    ASSERT_TRUE(reader.next_batch(2, rows)) << reader.error();
    ASSERT_EQ(2U, rows.size());
    EXPECT_EQ(padding + "\"]}[", rows[0][1].value);
    EXPECT_TRUE(rows[0][2].is_null);
    EXPECT_TRUE(rows[1][1].is_null);
    EXPECT_EQ("true", rows[1][2].value);
    ASSERT_TRUE(reader.next_batch(1, rows));
    EXPECT_TRUE(rows.empty());
    ASSERT_TRUE(reader.rescan());
  }
  write("[ ]\n");
  ASSERT_TRUE(reader.open(true, directory, path, Format::Json));
  ASSERT_TRUE(reader.infer(columns)) << reader.error();
  ASSERT_EQ(1U, columns.size());
  EXPECT_EQ("value", columns[0].name);
  std::vector<Row> rows;
  ASSERT_TRUE(reader.next_batch(1, rows));
  EXPECT_TRUE(rows.empty());
}

TEST_F(CsvTest, RejectsInvalidJsonArraysAndOversizedObjects)
{
  const std::string path = directory / "input.csv";
  CsvReader reader;
  std::vector<Column> columns;
  for (const char *bad : {"{}", "[1]", "[", "[{}","[{},]", "[{} {}]", "[] false",
                          R"([{"x":1,"x":2}])", R"([{"x":[{"y":"}"}]}])",
                          R"([{"x":"\q"}])"}) {
    write(bad);
    ASSERT_TRUE(reader.open(true, directory, path, Format::Json));
    EXPECT_FALSE(reader.infer(columns)) << bad;
  }
  const std::string start = R"({"x":")";
  const std::string end = R"("})";
  for (bool too_large : {false, true}) {
    write("[" + start + std::string(4 * 1024 * 1024 - start.size() - end.size() + too_large, 'x') + end + "]");
    ASSERT_TRUE(reader.open(true, directory, path, Format::Json));
    EXPECT_EQ(!too_large, reader.infer(columns)) << reader.error();
  }
}

TEST_F(CsvTest, GlobMergesColumnsAndRejectsChangedManifest)
{
  const std::string pattern = (directory / "*.csv").string();
  write("id,amount\n1,9007199254740993\n");
  const std::string later = (directory / "z.csv").string();
  {
    std::ofstream output(later);
    output << "amount,id,note\n1.5,2,late\n";
  }
  CsvReader reader;
  ASSERT_TRUE(reader.open(true, directory, pattern));
  std::vector<Column> columns;
  ASSERT_TRUE(reader.infer(columns)) << reader.error();
  ASSERT_EQ(3U, columns.size());
  EXPECT_EQ(Type::Text, columns[1].type);
  EXPECT_TRUE(columns[2].nullable);
  std::vector<Row> rows;
  ASSERT_TRUE(reader.next_batch(2, rows)) << reader.error();
  ASSERT_EQ(2U, rows.size());
  EXPECT_TRUE(rows[0][2].is_null);
  EXPECT_EQ("2", rows[1][0].value);
  ASSERT_TRUE(reader.rescan()) << reader.error();
  {
    std::ofstream output(later, std::ios::app);
    output << "2,3,changed\n";
  }
  EXPECT_FALSE(reader.rescan());
  unlink(later.c_str());
}

TEST_F(CsvTest, CsvExportDoesNotPublishOnWriteFailure)
{
  const auto path = directory / "result.csv";
  const pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    std::signal(SIGXFSZ, SIG_IGN);
    const struct rlimit limit {64, 64};
    if (setrlimit(RLIMIT_FSIZE, &limit) != 0) {
      _exit(1);
    }
    int result;
    {
      CsvExport writer;
      if (writer.open(directory, path, {}) != 0) {
        _exit(2);
      }
      result = writer.write_field(std::string(4096, 'x'), false);
      if (result == 0) {
        result = writer.flush();
      }
      if (result == 0) {
        result = writer.publish(directory);
      }
    }
    _exit(result != 0 && !fs::exists(path) ? 0 : 3);
  }
  int status = 0;
  ASSERT_EQ(child, waitpid(child, &status, 0));
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(0, WEXITSTATUS(status));
  EXPECT_TRUE(fs::is_empty(directory));
}
