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


#include "file_parquet_reader.h"

#if defined(__linux__)
#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <locale>
#include <cmath>
#include <limits>
#include <optional>
#include <string_view>
#include <variant>
#include <stdexcept>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <zlib.h>
#include "lib/compress/zstd_1_3_8/ob_zstd_wrapper.h"

namespace oceanbase::sql::file_scan
{
namespace
{
constexpr std::size_t max_metadata = 1024 * 1024;
constexpr std::size_t max_page = 1024 * 1024;
constexpr std::size_t max_page_header = 4096;
constexpr std::size_t max_page_rows = 1024 * 1024;
constexpr int32_t boolean_type = 0;
constexpr int32_t int32_type = 1;
constexpr int32_t int64_type = 2;
constexpr int32_t float_type = 4;
constexpr int32_t double_type = 5;
constexpr int32_t byte_array_type = 6;
constexpr int32_t fixed_byte_array_type = 7;
constexpr std::size_t max_columns = 32;
constexpr std::size_t max_dictionary_entries = 16384;
constexpr int32_t optional = 1;
constexpr int32_t required = 0;
constexpr int32_t plain = 0;
constexpr int32_t rle = 3;
void require(bool condition, const char *message)
{
  if (!condition) {
    throw std::runtime_error(message);
  }
}

class Input {
public:
  explicit Input(int fd) : fd_(fd)
  {
    require(fstat(fd_, &identity_) == 0 && S_ISREG(identity_.st_mode),
            "expected a regular file");
  }

  void set_cancel(CsvReader::Continue callback, void *context)
  {
    callback_ = callback;
    context_ = context;
  }

  void check_cancelled() const
  {
    require(callback_ == nullptr || callback_(context_), "Parquet scan cancelled");
  }

  void check_identity() const
  {
    struct stat current {};
    require(fstat(fd_, &current) == 0 && current.st_dev == identity_.st_dev
            && current.st_ino == identity_.st_ino && current.st_size == identity_.st_size
            && current.st_mtim.tv_sec == identity_.st_mtim.tv_sec
            && current.st_mtim.tv_nsec == identity_.st_mtim.tv_nsec
            && current.st_ctim.tv_sec == identity_.st_ctim.tv_sec
            && current.st_ctim.tv_nsec == identity_.st_ctim.tv_nsec, "file identity changed");
  }

  int64_t size() const { return identity_.st_size; }

  std::vector<uint8_t> read(int64_t offset, std::size_t count) const
  {
    check_cancelled();
    require(offset >= 0 && offset <= size() && count <= static_cast<uint64_t>(size() - offset),
            "read exceeds file bounds");
    std::vector<uint8_t> bytes(count);
    std::size_t filled = 0;
    while (filled < count) {
      const auto got = pread(fd_, bytes.data() + filled, count - filled, offset + filled);
      if (got < 0 && errno == EINTR) {
        check_cancelled();
      } else {
        require(got > 0, "truncated file or read error");
        filled += got;
      }
    }
    return bytes;
  }

private:
  int fd_;
  CsvReader::Continue callback_ = nullptr;
  void *context_ = nullptr;
  struct stat identity_ {};
};

class Cursor {
public:
  enum Type : uint8_t {
    Stop = 0, True = 1, False = 2, Byte = 3, I16 = 4, I32 = 5, I64 = 6, Double = 7,
    Binary = 8, List = 9, Set = 10, Map = 11, Struct = 12, Uuid = 13
  };

  explicit Cursor(const std::vector<uint8_t> &bytes) : data_(bytes.data()), size_(bytes.size()) {}
  Cursor(const uint8_t *data, std::size_t size) : data_(data), size_(size) {}

  std::size_t position() const { return pos_; }
  std::size_t remaining() const { return size_ - pos_; }
  const uint8_t *take(std::size_t n)
  {
    require(n <= remaining(), "truncated compact structure or page");
    const auto *value = pos_ == 0 ? data_ : data_ + pos_;
    pos_ += n;
    return value;
  }

  uint8_t byte() { return *take(1); }

  uint32_t little_u32()
  {
    const auto *p = take(4);
    return uint32_t(p[0]) | (uint32_t(p[1]) << 8) | (uint32_t(p[2]) << 16) | (uint32_t(p[3]) << 24);
  }

  uint64_t little_u64()
  {
    const uint64_t low = little_u32();
    return low | (uint64_t(little_u32()) << 32);
  }

  uint64_t varint()
  {
    uint64_t value = 0;
    for (unsigned shift = 0; shift <= 63; shift += 7) {
      const uint8_t b = byte();
      require(shift != 63 || b <= 1, "compact integer overflow");
      value |= uint64_t(b & 0x7f) << shift;
      if ((b & 0x80) == 0) {
        return value;
      }
    }
    throw std::runtime_error("invalid compact integer");
  }

  int64_t integer(uint8_t actual, Type expected)
  {
    require(actual == expected, "unexpected compact integer type");
    const uint64_t bits = varint();
    const int64_t value = static_cast<int64_t>(bits >> 1) ^ -static_cast<int64_t>(bits & 1);
    require(expected != I32 || (value >= INT32_MIN && value <= INT32_MAX),
            "compact i32 overflow");
    return value;
  }

  std::string text(uint8_t actual)
  {
    require(actual == Binary, "expected compact string");
    const auto count = varint();
    require(count <= remaining() && count <= 1024, "invalid column name length");
    return std::string(reinterpret_cast<const char *>(take(count)), count);
  }

  uint64_t collection(uint8_t actual, Type expected)
  {
    require(actual == List, "expected compact list");
    const auto header = byte();
    require((header & 0x0f) == expected, "unexpected list element type");
    return collection_size(header);
  }

  template <typename F>
  void fields(F consume)
  {
    int id = 0;
    for (;;) {
      const auto header = byte();
      if (header == Stop) {
        return;
      }
      const auto delta = header >> 4;
      const auto type = header & 0xf;
      const auto next = delta == 0 ? integer(I16, I16) : id + delta;
      require(next > 0 && next <= INT16_MAX && type > Stop && type <= Uuid,
              "invalid compact field");
      id = static_cast<int>(next);
      if (!consume(id, type)) {
        skip(type, 0, true);
      }
    }
  }

private:
  uint64_t collection_size(uint8_t header)
  {
    const auto count = (header >> 4) == 15 ? varint() : header >> 4;
    require(count <= remaining(), "compact collection exceeds available bytes");
    return count;
  }

  void skip(uint8_t type, unsigned depth, bool field)
  {
    require(depth < 32, "compact structure too deep");
    switch (type) {
      case True:
      case False:
        if (!field) {
          const auto value = byte();
          require(value == True || value == False, "invalid compact boolean");
        }
        break;
      case Byte: take(1); break;
      case I16:
      case I32:
      case I64: varint(); break;
      case Double: take(8); break;
      case Uuid: take(16); break;
      case Binary: {
        const auto count = varint();
        require(count <= remaining(), "invalid compact binary length");
        take(count);
        break;
      }
      case List:
      case Set: {
        const auto header = byte();
        const auto count = collection_size(header);
        for (uint64_t i = 0; i < count; ++i) {
          skip(header & 0xf, depth + 1, false);
        }
        break;
      }
      case Map: {
        const auto count = varint();
        require(count <= remaining() / 2, "invalid compact map length");
        if (count != 0) {
          const auto types = byte();
          for (uint64_t i = 0; i < count; ++i) {
            skip(types >> 4, depth + 1, false);
            skip(types & 0xf, depth + 1, false);
          }
        }
        break;
      }
      case Struct:
        for (;;) {
          const auto header = byte();
          if (header == Stop) {
            break;
          }
          if ((header >> 4) == 0) {
            varint();
          }
          skip(header & 0xf, depth + 1, true);
        }
        break;
      default: throw std::runtime_error("invalid compact value type");
    }
  }

  const uint8_t *data_;
  std::size_t size_;
  std::size_t pos_ = 0;
};

enum class Logical { None, String, Date, Timestamp, Decimal };

struct Annotation {
  Logical type = Logical::None;
  int64_t scale = -1;
  int64_t precision = -1;
  int64_t fraction_digits = -1;
  bool utc = false;
};

struct ParquetColumn {
  std::string name;
  int64_t type = -1;
  int64_t repetition = -1;
  int64_t children = -1;
  int64_t fixed_length = -1;
  Annotation annotation;
};

Annotation read_logical_type(Cursor &cursor)
{
  Annotation annotation;
  cursor.fields([&](int tag, uint8_t kind) {
    require(annotation.type == Logical::None && kind == Cursor::Struct, "invalid logical type union");
    switch (tag) {
      case 1:
        annotation.type = Logical::String;
        return false;
      case 5:
        annotation.type = Logical::Decimal;
        cursor.fields([&](int field, uint8_t type) {
          if (field == 1) {
            annotation.scale = cursor.integer(type, Cursor::I32);
          } else if (field == 2) {
            annotation.precision = cursor.integer(type, Cursor::I32);
          } else {
            return false;
          }
          return true;
        });
        return true;
      case 6:
        annotation.type = Logical::Date;
        return false;
      case 8: {
        annotation.type = Logical::Timestamp;
        bool has_utc = false;
        cursor.fields([&](int field, uint8_t type) {
          if (field == 1) {
            require(!has_utc && (type == Cursor::True || type == Cursor::False),
                    "invalid timestamp UTC annotation");
            has_utc = true;
            annotation.utc = type == Cursor::True;
          } else if (field == 2) {
            require(type == Cursor::Struct && annotation.fraction_digits == -1, "invalid timestamp unit");
            cursor.fields([&](int unit, uint8_t unit_type) {
              require(unit_type == Cursor::Struct && annotation.fraction_digits == -1
                      && unit >= 1 && unit <= 3, "unsupported timestamp unit");
              annotation.fraction_digits = unit * 3;
              return false;
            });
          } else {
            return false;
          }
          return true;
        });
        require(has_utc && annotation.fraction_digits != -1, "incomplete timestamp annotation");
        return true;
      }
      default: throw std::runtime_error("unsupported logical type");
    }
  });
  require(annotation.type != Logical::None, "empty logical type");
  return annotation;
}

ParquetColumn read_column(Cursor &cursor)
{
  ParquetColumn column;
  int64_t converted = -1;
  int64_t scale = -1;
  int64_t precision = -1;
  cursor.fields([&](int id, uint8_t type) {
    switch (id) {
      case 1: column.type = cursor.integer(type, Cursor::I32); return true;
      case 2: column.fixed_length = cursor.integer(type, Cursor::I32); return true;
      case 3: column.repetition = cursor.integer(type, Cursor::I32); return true;
      case 4: column.name = cursor.text(type); return true;
      case 5: column.children = cursor.integer(type, Cursor::I32); return true;
      case 6: converted = cursor.integer(type, Cursor::I32); return true;
      case 7: scale = cursor.integer(type, Cursor::I32); return true;
      case 8: precision = cursor.integer(type, Cursor::I32); return true;
      case 10:
        require(type == Cursor::Struct && column.annotation.type == Logical::None, "invalid logical type");
        column.annotation = read_logical_type(cursor);
        return true;
      default: return false;
    }
  });
  Annotation legacy;
  switch (converted) {
    case -1: break;
    case 0: legacy.type = Logical::String; break;
    case 5: legacy = {Logical::Decimal, scale, precision}; break;
    case 6: legacy.type = Logical::Date; break;
    case 9:
    case 10: legacy = {Logical::Timestamp, -1, -1, converted == 9 ? 3 : 6, true}; break;
    default: throw std::runtime_error("unsupported converted type");
  }
  auto &annotation = column.annotation;
  if (annotation.type == Logical::None) {
    annotation = legacy;
  } else if (legacy.type != Logical::None) {
    // A modern local timestamp may carry a legacy timestamp annotation.
    require(annotation.type == legacy.type && annotation.scale == legacy.scale
            && annotation.precision == legacy.precision && annotation.fraction_digits == legacy.fraction_digits,
            "inconsistent Parquet annotations");
  }
  return column;
}

void validate_annotation(const ParquetColumn &col)
{
  const auto &annotation = col.annotation;
  switch (annotation.type) {
    case Logical::None:
      require(col.type != fixed_byte_array_type, "unsupported unannotated fixed byte array");
      break;
    case Logical::String: require(col.type == byte_array_type, "invalid STRING annotation"); break;
    case Logical::Date: require(col.type == int32_type, "invalid DATE physical type"); break;
    case Logical::Timestamp: require(col.type == int64_type, "invalid TIMESTAMP physical type"); break;
    case Logical::Decimal: {
      require(annotation.precision > 0 && annotation.precision <= 38
              && annotation.scale >= 0 && annotation.scale <= annotation.precision,
              "unsupported DECIMAL precision or scale");
      require((col.type == int32_type && annotation.precision <= 9)
              || (col.type == int64_type && annotation.precision <= 18)
              || col.type == byte_array_type
              || (col.type == fixed_byte_array_type && col.fixed_length >= 1 && col.fixed_length <= 16),
              "unsupported DECIMAL physical type or width");
      if (col.type == fixed_byte_array_type) {
        const unsigned __int128 limit = (static_cast<unsigned __int128>(1) << (8 * col.fixed_length - 1)) - 1;
        unsigned __int128 power = 1;
        for (int64_t digit = 0; digit < annotation.precision; ++digit) {
          power *= 10;
        }
        require(power - 1 <= limit, "DECIMAL precision exceeds fixed byte width");
      }
      break;
    }
  }
}

struct Chunk {
  int64_t physical = -1;
  int64_t values = -1;
  int64_t data = -1;
  int64_t dictionary = -1;
  int64_t bytes = -1;
  int64_t codec = -1;
  std::string column;
};

struct Group {
  int64_t rows = -1;
  std::vector<Chunk> columns;
};

Chunk read_chunk(Cursor &cursor)
{
  Chunk chunk;
  cursor.fields([&](int id, uint8_t type) {
    require(id != 1 && id != 8 && id != 9, "external or encrypted column chunk unsupported");
    if (id != 3) {
      return false;
    }
    require(type == Cursor::Struct && chunk.codec == -1, "invalid column metadata");
    bool has_encodings = false;
    cursor.fields([&](int field, uint8_t kind) {
      switch (field) {
        case 1: chunk.physical = cursor.integer(kind, Cursor::I32); return true;
        case 2: {
          const auto count = cursor.collection(kind, Cursor::I32);
          has_encodings = count != 0;
          for (uint64_t i = 0; i < count; ++i) {
            const auto encoding = cursor.integer(Cursor::I32, Cursor::I32);
            require(encoding == plain || encoding == 2 || encoding == rle || encoding == 4 || encoding == 8,
                    "column encoding unsupported");
          }
          return true;
        }
        case 3:
          require(cursor.collection(kind, Cursor::Binary) == 1, "nested column path unsupported");
          chunk.column = cursor.text(Cursor::Binary);
          return true;
        case 4: chunk.codec = cursor.integer(kind, Cursor::I32); return true;
        case 5: chunk.values = cursor.integer(kind, Cursor::I64); return true;
        case 7: chunk.bytes = cursor.integer(kind, Cursor::I64); return true;
        case 9: chunk.data = cursor.integer(kind, Cursor::I64); return true;
        case 11:
          chunk.dictionary = cursor.integer(kind, Cursor::I64);
          require(chunk.dictionary >= 4, "invalid dictionary page offset");
          return true;
        default: return false;
      }
    });
    require(chunk.physical >= 0 && has_encodings, "invalid column metadata");
    return true;
  });
  return chunk;
}

Group read_group(Cursor &cursor)
{
  Group group;
  cursor.fields([&](int id, uint8_t type) {
    if (id == 1) {
      const auto count = cursor.collection(type, Cursor::Struct);
      require(count > 0 && count <= max_columns && group.columns.empty(), "invalid column chunk count");
      for (uint64_t i = 0; i < count; ++i) {
        group.columns.push_back(read_chunk(cursor));
      }
    } else if (id == 3) {
      group.rows = cursor.integer(type, Cursor::I64);
    } else {
      return false;
    }
    return true;
  });
  return group;
}

struct Metadata {
  std::vector<ParquetColumn> columns;
  std::vector<Group> groups;
};

Metadata read_metadata(const Input &input)
{
  require(input.size() >= 12, "truncated Parquet file");
  const auto head = input.read(0, 4);
  const auto tail = input.read(input.size() - 8, 8);
  Cursor trailer(tail);
  const auto size = trailer.little_u32();
  require(std::memcmp(head.data(), "PAR1", 4) == 0 && std::memcmp(trailer.take(4), "PAR1", 4) == 0,
          "expected unencrypted Parquet magic");
  require(size > 0 && size <= max_metadata && size <= input.size() - 12, "invalid or oversized footer");
  const int64_t footer_offset = input.size() - 8 - size;
  const auto footer = input.read(footer_offset, size);
  Cursor cursor(footer);
  Metadata metadata;
  int64_t version = -1;
  int64_t rows = -1;
  bool has_groups = false;
  cursor.fields([&](int id, uint8_t type) {
    switch (id) {
      case 1: version = cursor.integer(type, Cursor::I32); return true;
      case 2: {
        const auto count = cursor.collection(type, Cursor::Struct);
        require(count > 1 && count <= max_columns + 1 && metadata.columns.empty(), "invalid flat schema size");
        const auto root = read_column(cursor);
        require(root.type == -1 && root.children == static_cast<int64_t>(count - 1), "nested schema unsupported");
        for (uint64_t i = 1; i < count; ++i) {
          auto col = read_column(cursor);
          require(col.children == -1 && !col.name.empty()
                  && (col.repetition == required || col.repetition == optional),
                  "nested or repeated columns unsupported");
          require(col.type == boolean_type || col.type == int32_type || col.type == int64_type
                  || col.type == float_type || col.type == double_type || col.type == byte_array_type
                  || col.type == fixed_byte_array_type,
                  "unsupported physical type");
          validate_annotation(col);
          require(std::none_of(metadata.columns.begin(), metadata.columns.end(), [&](const ParquetColumn &other) {
            return other.name == col.name;
          }), "duplicate column name");
          metadata.columns.push_back(std::move(col));
        }
        return true;
      }
      case 3: rows = cursor.integer(type, Cursor::I64); return true;
      case 4: {
        require(!has_groups, "duplicate row group list");
        has_groups = true;
        const auto count = cursor.collection(type, Cursor::Struct);
        require(count <= 4096, "too many row groups");
        for (uint64_t i = 0; i < count; ++i) {
          input.check_cancelled();
          metadata.groups.push_back(read_group(cursor));
        }
        return true;
      }
      default: return false;
    }
  });
  require(cursor.remaining() == 0 && version > 0 && rows >= 0 && has_groups
          && !metadata.columns.empty(), "unsupported or invalid Parquet schema");
  int64_t counted = 0;
  std::vector<std::pair<int64_t, int64_t>> chunk_ranges;
  for (const auto &group : metadata.groups) {
    require(group.rows >= 0 && group.rows <= rows - counted && group.columns.size() == metadata.columns.size(),
            "invalid row group metadata");
    counted += group.rows;
    for (std::size_t i = 0; i < group.columns.size(); ++i) {
      const auto &chunk = group.columns[i];
      const auto &column = metadata.columns[i];
      const bool codec_ok = chunk.codec == 0 || chunk.codec == 1 || chunk.codec == 2 || chunk.codec == 6;
      if (!codec_ok) {
        throw std::runtime_error("unsupported codec: " + std::to_string(chunk.codec));
      }
      const int64_t start = chunk.dictionary < 0 ? chunk.data : chunk.dictionary;
      require(chunk.values == group.rows && chunk.column == column.name && chunk.physical == column.type
              && start >= 4 && start <= footer_offset && chunk.bytes >= 0
              && chunk.bytes <= footer_offset - start && chunk.data >= start && chunk.data <= start + chunk.bytes,
              "invalid column chunk metadata");
      if (chunk.bytes != 0) {
        chunk_ranges.emplace_back(start, start + chunk.bytes);
      }
    }
  }
  std::sort(chunk_ranges.begin(), chunk_ranges.end());
  for (std::size_t i = 1; i < chunk_ranges.size(); ++i) {
    require(chunk_ranges[i - 1].second <= chunk_ranges[i].first, "overlapping column chunks");
  }
  require(counted == rows, "row groups do not match footer row count");
  input.check_identity();
  return metadata;
}

struct Page {
  int64_t type = -1;
  int64_t bytes = -1;
  int64_t compressed_bytes = -1;
  int64_t values = -1;
  int64_t rows = -1;
  int64_t nulls = -1;
  int64_t encoding = -1;
  int64_t definition = -1;
  int64_t definition_bytes = -1;
  int64_t repetition_bytes = -1;
  bool compressed = true;
  int header = 0;
};

Page read_page(Cursor &cursor)
{
  Page page;
  cursor.fields([&](int id, uint8_t type) {
    switch (id) {
      case 1: page.type = cursor.integer(type, Cursor::I32); return true;
      case 2: page.bytes = cursor.integer(type, Cursor::I32); return true;
      case 3: page.compressed_bytes = cursor.integer(type, Cursor::I32); return true;
      case 5:
      case 7:
      case 8:
        require(type == Cursor::Struct && page.header == 0, "invalid page header");
        page.header = id;
        cursor.fields([&](int field, uint8_t kind) {
          if (field == 1) {
            page.values = cursor.integer(kind, Cursor::I32);
          } else if (id != 8 && field == 2) {
            page.encoding = cursor.integer(kind, Cursor::I32);
          } else if (id == 5 && field == 3) {
            page.definition = cursor.integer(kind, Cursor::I32);
          } else if (id == 8 && field >= 2 && field <= 6) {
            int64_t *targets[] = {&page.nulls, &page.rows, &page.encoding,
                                  &page.definition_bytes, &page.repetition_bytes};
            *targets[field - 2] = cursor.integer(kind, Cursor::I32);
          } else if (id == 8 && field == 7) {
            require(kind == Cursor::True || kind == Cursor::False, "invalid compressed flag");
            page.compressed = kind == Cursor::True;
          } else {
            return false;
          }
          return true;
        });
        return true;
      default: return false;
    }
  });
  require((page.type == 0 && page.header == 5) || (page.type == 2 && page.header == 7)
          || (page.type == 3 && page.header == 8), "unsupported page type or invalid header");
  require(page.bytes >= 0 && page.bytes <= static_cast<int64_t>(max_page)
          && page.compressed_bytes >= 0 && page.compressed_bytes <= static_cast<int64_t>(max_page)
          && page.values >= 0 && page.values <= static_cast<int64_t>(max_page_rows),
          "invalid or oversized page");
  return page;
}

class Hybrid {
public:
  Hybrid(Cursor source, unsigned width) : source_(source), packed_data_(nullptr, 0), width_(width)
  {
    require(width <= 32, "invalid hybrid bit width");
  }

  uint32_t next()
  {
    if (remaining_ == 0) {
      const uint64_t header = source_.varint();
      remaining_ = header >> 1;
      require(remaining_ != 0, "empty hybrid run");
      packed_ = (header & 1) != 0;
      if (packed_) {
        require(remaining_ <= max_page_rows / 8 && remaining_ * width_ <= source_.remaining(),
                "invalid bit-packed run length");
        const auto bytes = remaining_ * width_;
        packed_data_ = Cursor(source_.take(bytes), bytes);
        remaining_ *= 8;
        bits_ = 0;
        bit_count_ = 0;
      } else {
        require(remaining_ <= max_page_rows, "invalid RLE run length");
        repeat_ = 0;
        for (unsigned byte = 0; byte < (width_ + 7) / 8; ++byte) {
          repeat_ |= uint32_t(source_.byte()) << (byte * 8);
        }
        require(width_ == 32 || repeat_ < (uint32_t{1} << width_), "RLE value exceeds bit width");
      }
    }
    --remaining_;
    if (!packed_) {
      return repeat_;
    }
    while (bit_count_ < width_) {
      bits_ |= uint64_t(packed_data_.byte()) << bit_count_;
      bit_count_ += 8;
    }
    const auto value = static_cast<uint32_t>(bits_ & ((uint64_t{1} << width_) - 1));
    bits_ >>= width_;
    bit_count_ -= width_;
    return value;
  }

  void finish() const
  {
    require(source_.remaining() == 0 && (remaining_ == 0 || (packed_ && remaining_ <= 7)),
            "trailing hybrid levels or indices");
  }

private:
  Cursor source_;
  Cursor packed_data_;
  unsigned width_;
  uint64_t remaining_ = 0;
  uint64_t bits_ = 0;
  unsigned bit_count_ = 0;
  uint32_t repeat_ = 0;
  bool packed_ = false;
};

using Cell = std::variant<std::monostate, bool, int64_t, float, double, std::string_view>;

bool valid_utf8(std::string_view value)
{
  for (std::size_t i = 0; i < value.size();) {
    const auto lead = static_cast<uint8_t>(value[i++]);
    if (lead < 128) {
      continue;
    }
    if (lead < 0xc2 || lead > 0xf4) {
      return false;
    }
    const unsigned count = lead < 0xe0 ? 1 : lead < 0xf0 ? 2 : 3;
    if (value.size() - i < count) {
      return false;
    }
    const auto second = static_cast<uint8_t>(value[i]);
    if ((lead == 0xe0 && second < 0xa0) || (lead == 0xed && second >= 0xa0)
        || (lead == 0xf0 && second < 0x90) || (lead == 0xf4 && second >= 0x90)) {
      return false;
    }
    for (unsigned n = 0; n < count; ++n) {
      if ((static_cast<uint8_t>(value[i++]) & 0xc0) != 0x80) {
        return false;
      }
    }
  }
  return true;
}

Cell plain_value(Cursor &cursor, const ParquetColumn &column, unsigned &bool_bit, uint8_t &bool_bits)
{
  switch (column.type) {
    case boolean_type: {
      if (bool_bit == 8) {
        bool_bits = cursor.byte();
        bool_bit = 0;
      }
      return (bool_bits & (1u << bool_bit++)) != 0;
    }
    case int32_type: {
      const uint32_t value = cursor.little_u32();
      return value <= INT32_MAX ? int64_t(value) : int64_t(value) - (int64_t{1} << 32);
    }
    case int64_type: {
      const uint64_t value = cursor.little_u64();
      return value <= INT64_MAX ? int64_t(value) : -1 - int64_t(~value);
    }
    case float_type: {
      const auto bits = cursor.little_u32();
      float value;
      static_assert(sizeof(value) == sizeof(bits));
      std::memcpy(&value, &bits, sizeof(value));
      return value;
    }
    case double_type: {
      const auto bits = cursor.little_u64();
      double value;
      static_assert(sizeof(value) == sizeof(bits));
      std::memcpy(&value, &bits, sizeof(value));
      return value;
    }
    case byte_array_type:
    case fixed_byte_array_type: {
      const auto size = column.type == byte_array_type ? cursor.little_u32() : column.fixed_length;
      if (column.annotation.type == Logical::Decimal) {
        require(size > 0 && size <= 16, "unsupported DECIMAL byte width");
      }
      const auto *data = cursor.take(size);
      const std::string_view value(reinterpret_cast<const char *>(data), size);
      require(column.annotation.type == Logical::Decimal || valid_utf8(value), "BYTE_ARRAY is not UTF-8");
      return value;
    }
    default: throw std::runtime_error("unsupported PLAIN type");
  }
}

void decode_snappy(const Input &file, Cursor source, uint8_t *output, std::size_t capacity)
{
  const auto declared = source.varint();
  require(declared <= UINT32_MAX && declared == capacity, "Snappy output length mismatch");
  std::size_t written = 0;
  while (source.remaining() != 0) {
    file.check_cancelled();
    const unsigned tag = source.byte();
    const unsigned kind = tag & 3;
    if (kind == 0) {
      uint64_t length = tag >> 2;
      if (length >= 60) {
        const unsigned length_bytes = length - 59;
        length = 0;
        for (unsigned i = 0; i < length_bytes; ++i) {
          length |= uint64_t(source.byte()) << (8 * i);
        }
      }
      ++length;
      require(length <= capacity - written, "Snappy literal exceeds output");
      require(length <= source.remaining(), "truncated Snappy literal");
      std::memcpy(output + written, source.take(length), length);
      written += length;
    } else {
      unsigned length;
      uint32_t offset;
      if (kind == 1) {
        length = 4 + ((tag >> 2) & 7);
        offset = ((tag & 0xe0) << 3) | source.byte();
      } else {
        length = 1 + (tag >> 2);
        offset = 0;
        for (unsigned i = 0; i < (kind == 2 ? 2u : 4u); ++i) {
          offset |= uint32_t(source.byte()) << (8 * i);
        }
      }
      require(offset != 0 && offset <= written, "invalid Snappy copy offset");
      require(length <= capacity - written, "Snappy copy exceeds output");
      for (unsigned i = 0; i < length; ++i) {
        output[written] = output[written - offset];
        ++written;
      }
    }
  }
  require(written == capacity, "truncated Snappy block");
}

void decode_gzip(const Input &file, const uint8_t *input, std::size_t size, uint8_t *output, std::size_t capacity)
{
  std::size_t read = 0;
  std::size_t written = 0;
  require(size != 0, "missing GZIP member");
  while (read < size) {
    require(size - read >= 3 && input[read] == 0x1f && input[read + 1] == 0x8b
            && input[read + 2] == 8, "invalid GZIP member header");
    z_stream stream{};
    require(inflateInit2(&stream, 15 + 16) == Z_OK, "cannot initialize GZIP decoder");
    struct Cleanup {
      z_stream &stream;
      ~Cleanup() { inflateEnd(&stream); }
    } cleanup{stream};
    stream.next_in = const_cast<uint8_t *>(input + read);
    stream.avail_in = static_cast<uInt>(size - read);
    int status = Z_OK;
    while (status != Z_STREAM_END) {
      file.check_cancelled();
      uint8_t spare = 0;
      const std::size_t room = capacity - written;
      const auto window = static_cast<uInt>(std::min<std::size_t>(room == 0 ? 1 : room, 65536));
      stream.next_out = room == 0 ? &spare : output + written;
      stream.avail_out = window;
      const auto before = stream.avail_in;
      status = inflate(&stream, Z_NO_FLUSH);
      const auto produced = window - stream.avail_out;
      require(status != Z_BUF_ERROR, "truncated GZIP member");
      require(status == Z_OK || status == Z_STREAM_END, "invalid GZIP data or trailer");
      require(produced <= room, "GZIP output exceeds page");
      require(status == Z_STREAM_END || before != stream.avail_in || produced != 0, "truncated GZIP member");
      written += produced;
    }
    require(stream.total_in != 0, "empty GZIP member input");
    read += stream.total_in;
  }
  require(written == capacity, "GZIP output length mismatch");
}

std::vector<uint8_t> decode_payload(const Input &file, std::vector<uint8_t> input, const Page &page, int64_t codec,
                                    std::size_t levels)
{
  if (codec == 0 || (page.type == 3 && !page.compressed)) {
    require(input.size() == static_cast<std::size_t>(page.bytes), "uncompressed page size mismatch");
    return input;
  }
  require(levels <= input.size() && levels <= static_cast<std::size_t>(page.bytes),
          "invalid uncompressed level prefix");
  std::vector<uint8_t> result(page.bytes);
  std::copy_n(input.begin(), levels, result.begin());
  auto *output = levels == 0 ? result.data() : result.data() + levels;
  const auto *source = levels == 0 ? input.data() : input.data() + levels;
  const auto capacity = result.size() - levels;
  const auto size = input.size() - levels;
  if (codec == 1) {
    decode_snappy(file, Cursor(source, size), output, capacity);
    return result;
  }
  if (codec == 2) {
    decode_gzip(file, source, size, output, capacity);
    return result;
  }
  if (codec == 6) {
    using namespace oceanbase::common::zstd_1_3_8;
    OB_ZSTD_customMem memory{};
    char spare = 0;
    std::size_t decoded = 0;
    const int status = ObZstdWrapper::decompress(memory, reinterpret_cast<const char *>(source),
        size, capacity == 0 ? &spare : reinterpret_cast<char *>(output), capacity == 0 ? 1 : capacity, decoded);
    require(status == 0 && decoded == capacity, "invalid ZSTD page");
    return result;
  }
  throw std::runtime_error("unsupported codec");
}

class ColumnReader {
public:
  ColumnReader(const Input &input, const ParquetColumn &column, const Chunk &chunk, ParquetReader::PageCallback trace)
    : input_(input), column_(column), chunk_(chunk), trace_(trace),
      offset_(chunk.dictionary < 0 ? chunk.data : chunk.dictionary), end_(offset_ + chunk.bytes),
      values_(nullptr, 0) {}

  Cell next()
  {
    if (page_left_ == 0) {
      load_page();
    }
    --page_left_;
    ++rows_read_;
    Cell result;
    if (levels_ && levels_->next() == 0) {
      ++page_nulls_;
    } else if (encoded_) {
      const auto value = encoded_->next();
      if (encoding_ == rle) {
        result = value != 0;
      } else {
        require(value < dictionary_.size(), "dictionary index out of range");
        result = dictionary_[value];
      }
    } else {
      result = plain_value(values_, column_, bool_bit_, bool_bits_);
    }
    if (page_left_ == 0) {
      if (levels_) {
        levels_->finish();
      }
      if (encoded_) {
        encoded_->finish();
      }
      require(expected_nulls_ < 0 || expected_nulls_ == page_nulls_, "page null count mismatch");
      require(values_.remaining() == 0, "trailing page values");
    }
    return result;
  }

  void finish() const
  {
    require(page_left_ == 0 && offset_ == end_, "column chunk has trailing data or row count mismatch");
  }

private:
  void load_page()
  {
    input_.check_identity();
    for (;;) {
      const auto page_offset = offset_;
      require(offset_ < end_, "truncated column chunk");
      const auto prefix = input_.read(offset_, std::min<int64_t>(end_ - offset_, max_page_header));
      Cursor header(prefix);
      const auto page = read_page(header);
      offset_ += header.position();
      require(page.compressed_bytes <= end_ - offset_, "truncated data page");
      std::size_t definition_bytes = 0;
      if (page.type == 3) {
        require(page.rows == page.values && page.nulls >= 0 && page.nulls <= page.values
                && page.repetition_bytes == 0 && page.definition_bytes >= 0
                && page.definition_bytes <= page.bytes && page.definition_bytes <= page.compressed_bytes,
                "invalid flat DataPageV2 lengths or counts");
        definition_bytes = page.definition_bytes;
      }
      auto payload = decode_payload(input_, input_.read(offset_, page.compressed_bytes),
                                    page, chunk_.codec, definition_bytes);
      offset_ += page.compressed_bytes;
      input_.check_identity();
      if (page.type == 2) {
        require(!dictionary_seen_ && page_offset == chunk_.dictionary
                && page.values <= static_cast<int64_t>(max_dictionary_entries)
                && (page.encoding == plain || page.encoding == 2), "invalid dictionary page");
        dictionary_seen_ = true;
        dictionary_data_ = std::move(payload);
        Cursor dict(dictionary_data_);
        unsigned bit = 8;
        uint8_t bits = 0;
        for (int64_t i = 0; i < page.values; ++i) {
          dictionary_.push_back(plain_value(dict, column_, bit, bits));
        }
        require(dict.remaining() == 0, "trailing dictionary bytes");
      } else {
        require(page.values > 0 && (data_seen_ || page_offset == chunk_.data), "invalid data page offset or count");
        data_seen_ = true;
        if (trace_ != nullptr) {
          trace_(column_.name, rows_read_);
        }
        page_left_ = page.values;
        page_nulls_ = 0;
        expected_nulls_ = page.nulls;
        encoding_ = page.encoding;
        levels_.reset();
        encoded_.reset();
        bool_bit_ = 8;
        page_data_ = std::move(payload);
        values_ = Cursor(page_data_);
        if (column_.repetition == optional) {
          if (page.type == 0) {
            require(page.definition == rle, "definition level encoding unsupported");
            definition_bytes = values_.little_u32();
          }
          levels_.emplace(Cursor(values_.take(definition_bytes), definition_bytes), 1);
        } else {
          require(definition_bytes == 0 && page.nulls <= 0, "required column has definition levels or nulls");
        }
        if (encoding_ == 2 || encoding_ == 8) {
          require(dictionary_seen_, "dictionary data page without dictionary");
          if (values_.remaining() != 0) {
            const auto width = values_.byte();
            const auto size = values_.remaining();
            encoded_.emplace(Cursor(values_.take(size), size), width);
          }
        } else if (encoding_ == rle && column_.type == boolean_type) {
          if (values_.remaining() != 0) {
            const auto size = values_.little_u32();
            encoded_.emplace(Cursor(values_.take(size), size), 1);
          }
        } else {
          require(encoding_ == plain, "unsupported data page encoding");
        }
        return;
      }
    }
  }

  const Input &input_;
  const ParquetColumn &column_;
  const Chunk &chunk_;
  ParquetReader::PageCallback trace_;
  int64_t offset_;
  int64_t end_;
  int64_t page_left_ = 0;
  int64_t rows_read_ = 0;
  int64_t page_nulls_ = 0;
  int64_t expected_nulls_ = -1;
  int64_t encoding_ = -1;
  bool dictionary_seen_ = false;
  bool data_seen_ = false;
  unsigned bool_bit_ = 8;
  uint8_t bool_bits_ = 0;
  std::vector<uint8_t> dictionary_data_;
  std::vector<Cell> dictionary_;
  std::vector<uint8_t> page_data_;
  Cursor values_;
  std::optional<Hybrid> levels_;
  std::optional<Hybrid> encoded_;
};

void print_temporal(std::ostream &output, int64_t value, const Annotation &annotation)
{
  int64_t seconds;
  int64_t fraction = 0;
  if (annotation.type == Logical::Date) {
    seconds = value * 86400;
  } else {
    int64_t units = 1;
    for (int64_t i = 0; i < annotation.fraction_digits; ++i) {
      units *= 10;
    }
    seconds = value / units;
    fraction = value % units;
    if (fraction < 0) {
      --seconds;
      fraction += units;
    }
  }
  const time_t time = static_cast<time_t>(seconds);
  struct tm calendar {};
  require(static_cast<int64_t>(time) == seconds && gmtime_r(&time, &calendar) != nullptr
          && calendar.tm_year >= -1899 && calendar.tm_year <= 8099,
          "date or timestamp outside supported calendar years 0001-9999");
  char date[32];
  std::snprintf(date, sizeof(date), "%04d-%02d-%02d",
                calendar.tm_year + 1900, calendar.tm_mon + 1, calendar.tm_mday);
  output << date;
  if (annotation.type == Logical::Timestamp) {
    char clock[64];
    std::snprintf(clock, sizeof(clock), "T%02d:%02d:%02d.%0*lld%s",
                  calendar.tm_hour, calendar.tm_min, calendar.tm_sec,
                  static_cast<int>(annotation.fraction_digits), static_cast<long long>(fraction),
                  annotation.utc ? "Z" : "");
    output << clock;
  }
}

void print_decimal(std::ostream &output, const Cell &cell, const Annotation &annotation)
{
  using UInt = unsigned __int128;
  bool negative;
  UInt magnitude;
  if (const auto *integer = std::get_if<int64_t>(&cell)) {
    negative = *integer < 0;
    magnitude = negative ? UInt(-(*integer + 1)) + 1 : UInt(*integer);
  } else {
    const auto bytes = std::get<std::string_view>(cell);
    negative = (static_cast<uint8_t>(bytes.front()) & 0x80) != 0;
    magnitude = 0;
    for (const char byte : bytes) {
      magnitude = (magnitude << 8) | static_cast<uint8_t>(byte);
    }
    if (negative) {
      magnitude = ~magnitude + 1;
      if (bytes.size() < 16) {
        magnitude &= (UInt{1} << (bytes.size() * 8)) - 1;
      }
    }
  }
  std::string digits;
  do {
    digits.push_back(static_cast<char>('0' + magnitude % 10));
    magnitude /= 10;
  } while (magnitude != 0);
  require(digits.size() <= static_cast<std::size_t>(annotation.precision), "DECIMAL value exceeds precision");
  const auto scale = static_cast<std::size_t>(annotation.scale);
  if (scale != 0) {
    if (digits.size() <= scale) {
      digits.resize(scale + 1, '0');
    }
    digits.insert(scale, 1, '.');
  }
  if (negative) {
    digits.push_back('-');
  }
  std::reverse(digits.begin(), digits.end());
  output << digits;
}

Field to_field(const Cell &cell, const ParquetColumn &column)
{
  if (std::holds_alternative<std::monostate>(cell)) {
    return {{}, true};
  }
  if (column.annotation.type == Logical::None || column.annotation.type == Logical::String) {
    if (const auto *text = std::get_if<std::string_view>(&cell)) {
      return {std::string(*text), false, true};
    }
  }
  std::ostringstream output;
  output.imbue(std::locale::classic());
  if (column.annotation.type == Logical::Date || column.annotation.type == Logical::Timestamp) {
    print_temporal(output, std::get<int64_t>(cell), column.annotation);
  } else if (column.annotation.type == Logical::Decimal) {
    print_decimal(output, cell, column.annotation);
  } else {
    std::visit([&](const auto &value) {
      using T = std::decay_t<decltype(value)>;
      if constexpr (std::is_floating_point_v<T>) {
        require(std::isfinite(value), "non-finite Parquet floating-point value unsupported");
        output << std::setprecision(std::numeric_limits<double>::max_digits10) << static_cast<double>(value);
      } else if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, bool>) {
        output << value;
      }
    }, cell);
  }
  return {output.str(), false};
}
} // namespace

struct ParquetReader::State {
  explicit State(int fd, PageCallback page_trace) : input(fd), trace(page_trace) {}
  Input input;
  PageCallback trace;
  Metadata metadata;
  std::vector<ColumnReader> readers;
  std::size_t group = 0;
  int64_t row = 0;
};

ParquetReader::ParquetReader() = default;
ParquetReader::~ParquetReader() = default;

void ParquetReader::close()
{
  state_.reset();
  error_.clear();
}

bool ParquetReader::open(int fd, CsvReader::Continue should_continue, void *context, PageCallback trace)
{
  close();
  try {
    auto state = std::make_unique<State>(fd, trace);
    state->input.set_cancel(should_continue, context);
    state->metadata = read_metadata(state->input);
    state_ = std::move(state);
    return true;
  } catch (const std::exception &error) {
    error_ = error.what();
    return false;
  }
}

bool ParquetReader::infer_schema(std::vector<file_scan::Column> &columns)
{
  if (!state_) {
    error_ = "Parquet reader is not open";
    return false;
  }
  try {
    state_->input.check_identity();
    columns.clear();
    for (const auto &col : state_->metadata.columns) {
      require(valid_utf8(col.name) && col.name.find('\0') == col.name.npos, "invalid Parquet column name");
      Type type;
      switch (col.annotation.type) {
        case Logical::Date: type = Type::Date; break;
        case Logical::Timestamp:
          type = col.annotation.fraction_digits == 9 ? Type::Text : Type::DateTime;
          break;
        case Logical::Decimal: type = Type::Decimal; break;
        case Logical::String: type = Type::Text; break;
        default:
          type = col.type == boolean_type ? Type::Boolean
              : col.type == int32_type || col.type == int64_type ? Type::Integer
              : col.type == float_type || col.type == double_type ? Type::Double : Type::Text;
          break;
      }
      file_scan::Column column{col.name, type, col.repetition == optional};
      column.precision = col.annotation.precision;
      column.scale = col.annotation.scale;
      columns.push_back(std::move(column));
    }
    return true;
  } catch (const std::exception &error) {
    columns.clear();
    error_ = error.what();
    return false;
  }
}

bool ParquetReader::next_row(Row &row, CsvReader::Continue should_continue, void *context)
{
  row.clear();
  if (!state_) {
    error_ = "Parquet reader is not open";
    return false;
  }
  auto &state = *state_;
  try {
    state.input.set_cancel(should_continue, context);
    state.input.check_cancelled();
    state.input.check_identity();
    while (state.group < state.metadata.groups.size()) {
      const auto &group = state.metadata.groups[state.group];
      if (state.row == group.rows) {
        for (const auto &reader : state.readers) {
          reader.finish();
        }
        state.readers.clear();
        ++state.group;
        state.row = 0;
        continue;
      }
      if (state.readers.empty()) {
        state.readers.reserve(group.columns.size());
        for (std::size_t i = 0; i < group.columns.size(); ++i) {
          state.readers.emplace_back(state.input, state.metadata.columns[i], group.columns[i], state.trace);
        }
      }
      for (std::size_t i = 0; i < state.readers.size(); ++i) {
        row.push_back(to_field(state.readers[i].next(), state.metadata.columns[i]));
      }
      ++state.row;
      if (state.row == group.rows) {
        for (const auto &reader : state.readers) {
          reader.finish();
        }
      }
      state.input.check_identity();
      return true;
    }
    return true;
  } catch (const std::exception &error) {
    row.clear();
    error_ = error.what();
    return false;
  }
}

bool ParquetReader::rescan()
{
  if (!state_) {
    error_ = "Parquet reader is not open";
    return false;
  }
  try {
    state_->input.check_identity();
    state_->readers.clear();
    state_->row = 0;
    state_->group = 0;
    return true;
  } catch (const std::exception &error) {
    error_ = error.what();
    return false;
  }
}

} // namespace oceanbase::sql::file_scan

#else
namespace oceanbase::sql::file_scan
{
struct ParquetReader::State {};
ParquetReader::ParquetReader() = default;
ParquetReader::~ParquetReader() = default;
bool ParquetReader::open(int, CsvReader::Continue, void *, PageCallback)
{
  error_ = "Parquet FILE SCAN requires Linux";
  return false;
}
bool ParquetReader::infer_schema(std::vector<Column> &) { return false; }
bool ParquetReader::next_row(Row &, CsvReader::Continue, void *) { return false; }
bool ParquetReader::rescan() { return false; }
void ParquetReader::close() {}
} // namespace oceanbase::sql::file_scan
#endif
