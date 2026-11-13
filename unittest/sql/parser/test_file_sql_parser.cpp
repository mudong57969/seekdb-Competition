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

#include <gtest/gtest.h>
#include "lib/allocator/page_arena.h"
#include "sql/parser/ob_parser.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

static const ParseNode *find_csv(const ParseNode *node)
{
  if (node != nullptr) {
    if (node->type_ == T_READ_CSV_TABLE) {
      return node;
    }
    for (int i = 0; i < node->num_child_; ++i) {
      if (const ParseNode *found = find_csv(node->children_[i])) {
        return found;
      }
    }
  }
  return nullptr;
}

TEST(FileSqlParser, ReadsLiteralAndAlias)
{
  ObArenaAllocator allocator;
  ObParser parser(allocator, 0);
  for (const char *sql : {
      "SELECT * FROM read_csv('/allowed/test.csv')",
      "SELECT c.name FROM READ_CSV('/allowed/test.csv') AS c WHERE c.age > 18 LIMIT 5",
      "SELECT c.name FROM read_csv('/allowed/test.csv') c JOIN t ON t.name=c.name"}) {
    ParseResult result{};
    ASSERT_EQ(0, parser.parse(ObString::make_string(sql), result)) << sql;
    const ParseNode *node = find_csv(result.result_tree_);
    ASSERT_NE(nullptr, node);
    ASSERT_EQ(1, node->num_child_);
    EXPECT_STREQ("/allowed/test.csv", node->children_[0]->str_value_);
  }
}

TEST(FileSqlParser, KeepsOrdinaryIdentifiersAndRejectsOtherCalls)
{
  ObArenaAllocator allocator;
  ObParser parser(allocator, 0);
  ParseResult result{};
  ASSERT_EQ(0, parser.parse(ObString::make_string(
      "SELECT read_csv.id FROM read_csv JOIN t ON read_csv.id=t.id"), result));
  EXPECT_EQ(nullptr, find_csv(result.result_tree_));
  ParseResult scalar{};
  ASSERT_EQ(0, parser.parse(ObString::make_string("SELECT read_csv(1)"), scalar));
  EXPECT_EQ(nullptr, find_csv(scalar.result_tree_));
  for (const char *sql : {
      "SELECT * FROM read_csv(123)",
      "SELECT * FROM read_csv('/x','/y')",
      "SELECT * FROM not_csv('/x')"}) {
    ParseResult invalid{};
    EXPECT_NE(0, parser.parse(ObString::make_string(sql), invalid)) << sql;
  }
}

TEST(FileSqlParser, ReadsCsvNamedOptions)
{
  ObArenaAllocator allocator;
  ObParser parser(allocator, 0);
  for (const char *sql : {
      "SELECT * FROM read_csv('/allowed/test.csv', header = true, sample_size = -1)",
      "SELECT f.id FROM read_csv('/allowed/test.csv', sample_size = 10) AS f",
      "SELECT f.id FROM read_csv('/allowed/test.csv', HEADER = TRUE) f"}) {
    ParseResult result{};
    ASSERT_EQ(0, parser.parse(ObString::make_string(sql), result)) << sql;
    const ParseNode *node = find_csv(result.result_tree_);
    ASSERT_NE(nullptr, node);
    ASSERT_EQ(2, node->num_child_);
    EXPECT_TRUE(node->children_[1]->is_tree_not_param_);
  }
}

TEST(FileSqlParser, ReadsNdjsonTableSource)
{
  ObArenaAllocator allocator;
  ObParser parser(allocator, 0);
  ParseResult result{};
  ASSERT_EQ(0, parser.parse(ObString::make_string(
      "SELECT n.id FROM read_ndjson('/allowed/data.ndjson') AS n LIMIT 1"), result));
  const ParseNode *node = find_csv(result.result_tree_);
  ASSERT_NE(nullptr, node);
  EXPECT_STREQ("ndjson", node->str_value_);
  EXPECT_STREQ("/allowed/data.ndjson", node->children_[0]->str_value_);
  ParseResult csv{};
  ASSERT_EQ(0, parser.parse(ObString::make_string(
      "SELECT * FROM (read_csv('/allowed/data.csv'))"), csv));
  const ParseNode *csv_node = find_csv(csv.result_tree_);
  ASSERT_NE(nullptr, csv_node);
  EXPECT_EQ(nullptr, csv_node->str_value_);
}

TEST(FileSqlParser, ReadsJsonArrayTableSource)
{
  ObArenaAllocator allocator;
  ObParser parser(allocator, 0);
  ParseResult scalar{};
  ASSERT_EQ(0, parser.parse(ObString::make_string("SELECT read_ndjson(1), read_json(1)"), scalar));
  EXPECT_EQ(nullptr, find_csv(scalar.result_tree_));
  for (const char *sql : {
      "SELECT * FROM read_json('/allowed/data.json')",
      "SELECT j.id FROM read_json('/allowed/data.json') j",
      "SELECT j.id FROM read_json('/allowed/data.json') AS j"}) {
    ParseResult result{};
    ASSERT_EQ(0, parser.parse(ObString::make_string(sql), result)) << sql;
    const ParseNode *node = find_csv(result.result_tree_);
    ASSERT_NE(nullptr, node);
    EXPECT_STREQ("json", node->str_value_);
  }
}

TEST(FileSqlParser, ReadsParquetTableSource)
{
  ObArenaAllocator allocator;
  ObParser parser(allocator, 0);
  ParseResult result{};
  ASSERT_EQ(0, parser.parse(ObString::make_string(
      "SELECT p.id FROM read_parquet('/allowed/data.parquet') AS p WHERE p.id > 0 LIMIT 5"), result));
  const ParseNode *node = find_csv(result.result_tree_);
  ASSERT_NE(nullptr, node);
  EXPECT_STREQ("parquet", node->str_value_);
}

TEST(FileSqlParser, ParsesCopyWithoutReservingCopy)
{
  ObArenaAllocator allocator;
  ObParser parser(allocator, 0);
  for (const char *sql : {
      "COPY (SELECT 1) TO '/allowed/out.csv' WITH (FORMAT CSV, HEADER FALSE)",
      "COPY (SELECT id FROM read_csv('/allowed/in.csv')) TO '/allowed/out.csv' "
      "WITH (FORMAT CSV, HEADER TRUE)",
      "EXPLAIN copy", "SELECT csv(1), header(1)"}) {
    ParseResult result{};
    EXPECT_EQ(0, parser.parse(ObString::make_string(sql), result)) << sql;
  }
  ParseResult invalid{};
  EXPECT_NE(0, parser.parse(ObString::make_string(
      "COPY (SELECT 1 INTO @x) TO '/allowed/out.csv' WITH (FORMAT CSV, HEADER TRUE)"), invalid));
}
