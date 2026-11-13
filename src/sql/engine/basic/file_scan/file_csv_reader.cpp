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

#include "file_csv_reader.h"
#include "file_parquet_reader.h"

#include <rapidjson/reader.h>
#include <rapidjson/memorystream.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <string_view>
#include <unordered_set>
#include <utility>

#if defined(__linux__)
#include <dirent.h>
#include <fcntl.h>
#include <linux/openat2.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace oceanbase::sql::file_scan
{
namespace
{
constexpr std::size_t MAX_RECORD_BYTES = 4 * 1024 * 1024;
constexpr std::size_t MAX_COLUMNS = 4096;
constexpr std::size_t MAX_BATCH_ROWS = 32;
constexpr std::size_t MAX_GLOB_FILES = 4096;
constexpr std::string_view GLOB_PREFIX = "glob:";
constexpr std::string_view NDJSON_IDENTITY_PREFIX = "ndjson:";
constexpr std::string_view PARQUET_IDENTITY_PREFIX = "parquet:";
constexpr std::string_view JSON_IDENTITY_PREFIX = "json:";

#if defined(__linux__) && defined(SYS_openat2)
bool safe_absolute_path(std::string_view path)
{
  if (path.size() < 2 || path.front() != '/' || path.find('\0') != path.npos) {
    return false;
  }
  path.remove_prefix(1);
  while (!path.empty()) {
    const auto end = path.find('/');
    const auto component = path.substr(0, end);
    if (component.empty() || component == "." || component == "..") {
      return false;
    }
    if (end == path.npos) {
      return true;
    }
    path.remove_prefix(end + 1);
  }
  return false;
}

int open_beneath(int root, const char *path, unsigned flags, unsigned resolve)
{
  struct open_how options {};
  options.flags = flags | O_CLOEXEC;
  options.resolve = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | resolve;
  return static_cast<int>(syscall(SYS_openat2, root, path, &options, sizeof(options)));
}

int open_allowed_directory(const std::string &directory)
{
  const int root = open("/", O_PATH | O_DIRECTORY | O_CLOEXEC);
  if (root < 0) {
    return -1;
  }
  const int allowed = open_beneath(root, directory.c_str() + 1, O_PATH | O_DIRECTORY, 0);
  close(root);
  return allowed;
}

std::FILE *open_regular_file(const std::string &directory, const std::string &path)
{
  const int allowed = open_allowed_directory(directory);
  if (allowed < 0) {
    return nullptr;
  }
  // Pin the object before checking its type. O_PATH does not open a device or FIFO for I/O.
  const int candidate = open_beneath(allowed, path.c_str() + directory.size() + 1,
                                     O_PATH, RESOLVE_NO_XDEV);
  close(allowed);
  if (candidate < 0) {
    return nullptr;
  }
  struct stat status {};
  int readable = -1;
  if (fstat(candidate, &status) == 0 && S_ISREG(status.st_mode)) {
    const std::string descriptor = "/proc/self/fd/" + std::to_string(candidate);
    readable = open(descriptor.c_str(), O_RDONLY | O_CLOEXEC);
  }
  close(candidate);
  std::FILE *file = readable < 0 ? nullptr : fdopen(readable, "rb");
  if (file == nullptr && readable >= 0) {
    close(readable);
  }
  return file;
}
#endif

bool valid_utf8(std::string_view value)
{
  for (std::size_t pos = 0; pos < value.size();) {
    const auto lead = static_cast<unsigned char>(value[pos++]);
    if (lead < 0x80) {
      continue;
    }
    if (lead < 0xc2 || lead > 0xf4) {
      return false;
    }
    const unsigned count = lead < 0xe0 ? 1 : lead < 0xf0 ? 2 : 3;
    if (value.size() - pos < count) {
      return false;
    }
    const auto second = static_cast<unsigned char>(value[pos]);
    if ((lead == 0xe0 && second < 0xa0) || (lead == 0xed && second >= 0xa0)
        || (lead == 0xf0 && second < 0x90) || (lead == 0xf4 && second >= 0x90)) {
      return false;
    }
    for (unsigned i = 0; i < count; ++i) {
      if ((static_cast<unsigned char>(value[pos++]) & 0xc0) != 0x80) {
        return false;
      }
    }
  }
  return true;
}

template <typename Number>
bool parse_number(std::string_view input, Number &number)
{
  if (input.empty()) {
    return false;
  }
  const char *end = input.data() + input.size();
  const auto result = std::from_chars(input.data(), end, number);
  return result.ec == std::errc{} && result.ptr == end;
}

#if defined(__linux__)
struct DecimalKey {
  std::string digits;
  std::int64_t exponent = 0;
  bool negative = false;
};

// The decimal keys allow equivalent spellings while requiring a DOUBLE to
// round-trip the input's significant decimal digits.
bool decimal_key(std::string_view input, DecimalKey &key)
{
  key.negative = input.front() == '-';
  if (key.negative) {
    input.remove_prefix(1);
  }
  const auto exp_pos = input.find_first_of("eE");
  if (exp_pos != input.npos) {
    auto exponent = input.substr(exp_pos + 1);
    if (!exponent.empty() && exponent.front() == '+') {
      exponent.remove_prefix(1);
    }
    if (!parse_number(exponent, key.exponent)
        || key.exponent > static_cast<std::int64_t>(MAX_RECORD_BYTES) || key.exponent < -static_cast<std::int64_t>(MAX_RECORD_BYTES)) {
      return false;
    }
    input = input.substr(0, exp_pos);
  }
  const auto dot = input.find('.');
  if (dot == input.npos) {
    key.digits = input;
  } else {
    key.digits = std::string(input.substr(0, dot));
    key.digits.append(input.substr(dot + 1));
    key.exponent -= static_cast<std::int64_t>(input.size() - dot - 1);
  }
  const auto first = key.digits.find_first_not_of('0');
  if (first == key.digits.npos) {
    key.digits = "0";
    key.exponent = 0;
  } else {
    const auto last = key.digits.find_last_not_of('0');
    key.exponent += static_cast<std::int64_t>(key.digits.size() - last - 1);
    key.digits = key.digits.substr(first, last - first + 1);
  }
  return true;
}

bool parse_double(std::string_view input, double &number)
{
  if (!parse_number(input, number) || !std::isfinite(number)) {
    return false;
  }
  char buffer[64];
  const auto written = std::to_chars(buffer, buffer + sizeof(buffer), number);
  DecimalKey original;
  DecimalKey roundtrip;
  return written.ec == std::errc{} && decimal_key(input, original)
      && decimal_key(std::string_view(buffer, written.ptr - buffer), roundtrip)
      && original.negative == roundtrip.negative && original.exponent == roundtrip.exponent
      && original.digits == roundtrip.digits;
}

#else
bool parse_double(std::string_view, double &)
{
  return false;
}
#endif

bool digits_at(std::string_view input, std::size_t offset, std::size_t count, int &value)
{
  value = 0;
  for (std::size_t i = offset; i < offset + count; ++i) {
    const char ch = input[i];
    if (ch < '0' || ch > '9') {
      return false;
    }
    value = value * 10 + ch - '0';
  }
  return true;
}

int days_before_year(int year)
{
  const int y = year - 1;
  return 365 * y + y / 4 - y / 100 + y / 400;
}

bool parse_temporal(Type type, std::string_view input, std::int64_t &value)
{
  int year, month, day;
  if (input.size() < 10 || input[4] != '-' || input[7] != '-'
      || !digits_at(input, 0, 4, year) || !digits_at(input, 5, 2, month)
      || !digits_at(input, 8, 2, day) || year < 1 || month < 1 || month > 12 || day < 1) {
    return false;
  }
  constexpr int MONTH_START[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
  const bool leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
  if (day > MONTH_START[month] - MONTH_START[month - 1] + (leap && month == 2)) {
    return false;
  }
  const int days = days_before_year(year) - days_before_year(1970)
      + MONTH_START[month - 1] + (leap && month > 2) + day - 1;
  if (type == Type::Date) {
    value = days;
    return input.size() == 10;
  }

  constexpr std::int64_t MICROS = 1000000;
  value = days * 86400LL * MICROS;
  if (input.size() == 10) {
    return true;
  }
  int hour, minute, second;
  if (input.size() < 19 || input.size() > 26 || (input[10] != ' ' && input[10] != 'T')
      || input[13] != ':' || input[16] != ':' || !digits_at(input, 11, 2, hour)
      || !digits_at(input, 14, 2, minute) || !digits_at(input, 17, 2, second)
      || hour >= 24 || minute >= 60 || second >= 60) {
    return false;
  }
  value += (hour * 3600 + minute * 60 + second) * MICROS;
  if (input.size() > 19) {
    int fraction;
    if (input[19] != '.' || input.size() == 20 || !digits_at(input, 20, input.size() - 20, fraction)) {
      return false;
    }
    for (std::size_t i = input.size(); i < 26; ++i) {
      fraction *= 10;
    }
    value += fraction;
  }
  return true;
}

bool equals_lowercase(std::string_view input, std::string_view lowercase)
{
  if (input.size() != lowercase.size()) {
    return false;
  }
  for (std::size_t i = 0; i < input.size(); ++i) {
    const char ch = input[i];
    if ((ch >= 'A' && ch <= 'Z' ? ch + 'a' - 'A' : ch) != lowercase[i]) {
      return false;
    }
  }
  return true;
}

Type classify(std::string_view input)
{
  Scalar value;
  for (const Type type : {Type::Integer, Type::Double, Type::Boolean, Type::Date, Type::DateTime}) {
    if (parse_scalar(type, input, value)) {
      return type;
    }
  }
  return Type::Text;
}

Type merge_type(Type previous, Type current, bool double_compatible)
{
  if (previous == Type::Null || previous == current) {
    return current;
  }
  if ((previous == Type::Integer && current == Type::Double)
      || (previous == Type::Double && current == Type::Integer)) {
    return double_compatible ? Type::Double : Type::Text;
  }
  if ((previous == Type::Date && current == Type::DateTime)
      || (previous == Type::DateTime && current == Type::Date)) {
    return Type::DateTime;
  }
  return Type::Text;
}
void infer_field(Column &column, const Field &field, bool &double_compatible)
{
  column.nullable |= field.is_null;
  if (!field.is_null && column.type != Type::Text) {
    const Type current = field.json_string ? Type::Text : classify(field.value);
    if (current == Type::Integer && double_compatible) {
      Scalar number;
      double_compatible = parse_scalar(Type::Double, field.value, number);
    }
    column.type = merge_type(column.type, current, double_compatible);
  }
}

class ObjectSax : public rapidjson::BaseReaderHandler<rapidjson::UTF8<>, ObjectSax>
{
public:
  explicit ObjectSax(std::vector<std::pair<std::string, Field>> &fields) : fields_(fields) {}
  bool StartObject()
  {
    if (started_) {
      return false;
    }
    started_ = true;
    return true;
  }
  bool EndObject(rapidjson::SizeType) { return started_; }
  bool StartArray() { return false; }
  bool Key(const char *text, rapidjson::SizeType size, bool)
  {
    const std::string_view key(text, size);
    if (key.empty() || size > 128 || key.find('\0') != key.npos || fields_.size() >= MAX_COLUMNS
        || !keys_.emplace(key).second) {
      return false;
    }
    key_.assign(key);
    return true;
  }
  bool Null() { return add({}, true, false); }
  bool Bool(bool value) { return add(value ? "true" : "false", false, false); }
  bool RawNumber(const char *text, rapidjson::SizeType size, bool)
  {
    return add(std::string(text, size), false, false);
  }
  bool String(const char *text, rapidjson::SizeType size, bool)
  {
    return add(std::string(text, size), false, true);
  }

private:
  bool add(std::string value, bool is_null, bool json_string)
  {
    if (!started_ || key_.empty()) {
      return false;
    }
    fields_.emplace_back(std::move(key_), Field{std::move(value), is_null, json_string});
    key_.clear();
    return true;
  }
  std::vector<std::pair<std::string, Field>> &fields_;
  std::unordered_set<std::string> keys_;
  std::string key_;
  bool started_ = false;
};
} // namespace

Format format_from_identity(std::string_view identity)
{
  if (identity.substr(0, 11) == "glob:files:") {
    return Format::Glob;
  }
  if (identity.substr(0, GLOB_PREFIX.size()) == GLOB_PREFIX) {
    identity.remove_prefix(GLOB_PREFIX.size());
  }
  if (identity.substr(0, PARQUET_IDENTITY_PREFIX.size()) == PARQUET_IDENTITY_PREFIX) {
    return Format::Parquet;
  }
  if (identity.substr(0, JSON_IDENTITY_PREFIX.size()) == JSON_IDENTITY_PREFIX) {
    return Format::Json;
  }
  return identity.substr(0, NDJSON_IDENTITY_PREFIX.size()) == NDJSON_IDENTITY_PREFIX
      ? Format::Ndjson : Format::Csv;
}

bool parse_scalar(Type type, std::string_view input, Scalar &value)
{
  switch (type) {
    case Type::Integer:
      return parse_number(input, value.integer);
    case Type::Double:
      return parse_double(input, value.real);
    case Type::Boolean:
      if (equals_lowercase(input, "true")) {
        value.integer = 1;
        return true;
      }
      value.integer = 0;
      return equals_lowercase(input, "false");
    case Type::Date:
    case Type::DateTime:
      return parse_temporal(type, input, value.integer);
    default:
      return false;
  }
}

namespace
{
bool beneath_or_same(std::string_view path, std::string_view root)
{
  return !root.empty() && (path == root
      || (path.size() > root.size() && path.substr(0, root.size()) == root && path[root.size()] == '/'));
}

void trim_directory(std::string &path)
{
  while (path.size() > 1 && path.back() == '/') {
    path.pop_back();
  }
}
} // namespace

CsvExport::~CsvExport()
{
  if (file_ != nullptr) {
    std::fclose(file_);
  }
#if defined(__linux__)
  if (parent_ >= 0) {
    ::close(parent_);
  }
#endif
}

int CsvExport::open_parent(const std::string &allowed) const
{
#if defined(__linux__) && defined(SYS_openat2)
  if (!safe_absolute_path(allowed) || !beneath_or_same(directory_, allowed)) {
    errno = EACCES;
    return -1;
  }
  const int root = open_allowed_directory(allowed);
  if (root < 0) {
    return -1;
  }
  const int dir = open_beneath(root, directory_ == allowed ? "." : directory_.c_str() + allowed.size() + 1,
                               O_PATH | O_DIRECTORY, RESOLVE_NO_XDEV);
  const int saved_error = errno;
  ::close(root);
  errno = saved_error;
  return dir;
#else
  (void)allowed;
  return -1;
#endif
}

int CsvExport::open(std::string allowed, const std::string &path,
                    const std::vector<std::string> &protected_directories)
{
#if defined(__linux__) && defined(SYS_openat2)
  if (file_ != nullptr || parent_ >= 0 || !safe_absolute_path(path)) {
    return EACCES;
  }
  trim_directory(allowed);
  const auto split = path.rfind('/');
  directory_ = path.substr(0, split);
  name_ = path.substr(split + 1);
  if (name_.size() <= 4 || name_.compare(name_.size() - 4, 4, ".csv") != 0) {
    return EINVAL;
  }
  for (const auto root : {"/bin", "/boot", "/dev", "/etc", "/lib", "/lib64", "/proc",
                          "/run", "/sbin", "/sys", "/usr", "/var/lib", "/var/log"}) {
    if (beneath_or_same(directory_, root)) {
      return EACCES;
    }
  }
  for (const auto &root : protected_directories) {
    if (root == "/" || beneath_or_same(directory_, root)) {
      return EACCES;
    }
  }
  parent_ = open_parent(allowed);
  if (parent_ < 0) {
    return EACCES;
  }
  struct stat existing {};
  if (fstatat(parent_, name_.c_str(), &existing, AT_SYMLINK_NOFOLLOW) == 0) {
    return EEXIST;
  }
  if (errno != ENOENT) {
    return errno;
  }
  const int fd = openat(parent_, ".", O_TMPFILE | O_WRONLY | O_CLOEXEC, 0600);
  if (fd < 0) {
    return errno;
  }
  file_ = fdopen(fd, "wb");
  if (file_ == nullptr) {
    const int saved_error = errno;
    ::close(fd);
    return saved_error;
  }
  return 0;
#else
  (void)allowed;
  (void)path;
  (void)protected_directories;
  return ENOTSUP;
#endif
}

int CsvExport::write_separator(char ch)
{
  return std::fputc(ch, file_) == EOF ? EIO : 0;
}

int CsvExport::write_field(std::string_view value, bool quoted)
{
  if (!valid_utf8(value) || value.find('\0') != value.npos) {
    return EILSEQ;
  }
  quoted |= value.empty() || value.find_first_of(",\"\r\n") != value.npos;
  if (quoted && write_separator('"') != 0) {
    return EIO;
  }
  for (;;) {
    const auto quote = value.find('"');
    const auto part = value.substr(0, quote);
    if (!part.empty() && std::fwrite(part.data(), 1, part.size(), file_) != part.size()) {
      return EIO;
    }
    if (quote == value.npos) {
      break;
    }
    if (std::fwrite("\"\"", 1, 2, file_) != 2) {
      return EIO;
    }
    value.remove_prefix(quote + 1);
  }
  return quoted ? write_separator('"') : 0;
}

int CsvExport::flush()
{
#if defined(__linux__) && defined(SYS_openat2)
  return std::fflush(file_) != 0 || fsync(fileno(file_)) != 0 ? errno : 0;
#else
  return ENOTSUP;
#endif
}

int CsvExport::publish(std::string allowed)
{
#if defined(__linux__) && defined(SYS_openat2)
  trim_directory(allowed);
  const int current = open_parent(allowed);
  if (current < 0) {
    return EACCES;
  }
  struct stat original {}, now {};
  int result = EACCES;
  if (fstat(parent_, &original) == 0 && fstat(current, &now) == 0
      && original.st_dev == now.st_dev && original.st_ino == now.st_ino) {
    const std::string descriptor = "/proc/self/fd/" + std::to_string(fileno(file_));
    result = linkat(AT_FDCWD, descriptor.c_str(), current, name_.c_str(), AT_SYMLINK_FOLLOW) == 0 ? 0 : errno;
  }
  ::close(current);
  return result;
#else
  (void)allowed;
  return ENOTSUP;
#endif
}

CsvReader::CsvReader() = default;

CsvReader::~CsvReader()
{
  close();
}

void CsvReader::close()
{
  parquet_.reset();
  parquet_columns_.clear();
  if (file_ != nullptr) {
    std::fclose(file_);
    file_ = nullptr;
  }
  data_offset_ = -1;
  width_ = 0;
  json_indexes_.clear();
  csv_header_.clear();
  glob_member_ = false;
  glob_pattern_.clear();
  glob_directory_.clear();
  glob_allowed_.clear();
  glob_files_.clear();
  glob_names_.clear();
  glob_mapping_.clear();
  glob_reader_.reset();
  glob_index_ = 0;
  identity_.clear();
}

bool CsvReader::fail(const char *message)
{
  error_ = message;
  return false;
}

bool CsvReader::open(bool enabled, const std::string &directory, const std::string &path, Format format,
                     Continue should_continue, void *context)
{
  close();
  format_ = format;
  error_.clear();
  if (should_continue != nullptr && !should_continue(context)) {
    return fail("file open cancelled");
  }
  if (!enabled) {
    return fail("file SQL is disabled");
  }
#if defined(__linux__) && defined(SYS_openat2)
  // Permit a trailing separator in the configured directory, while requiring an explicit root.
  std::string allowed = directory;
  while (allowed.size() > 1 && allowed.back() == '/') {
    allowed.pop_back();
  }
  if (!safe_absolute_path(allowed) || !safe_absolute_path(path)
      || path.size() <= allowed.size() || path.compare(0, allowed.size(), allowed) != 0
      || path[allowed.size()] != '/') {
    return fail("CSV path is outside the configured secure directory or is invalid");
  }
  const std::string_view filename = std::string_view(path).substr(path.rfind('/') + 1);
  if (format == Format::Glob) {
    return filename == "*" ? open_glob(allowed, path, should_continue, context)
                           : fail("glob() requires a final * component");
  }
  if (filename.size() > 2 && filename.substr(0, 2) == "*."
      && filename.substr(2).find_first_of("*?[]") == filename.npos) {
    return format != Format::Parquet ? open_glob(allowed, path, should_continue, context)
                                     : fail("Parquet glob unsupported");
  }
  return open_single(allowed, path);
#else
  (void)directory;
  (void)path;
  (void)should_continue;
  (void)context;
  return fail("secure file opening requires Linux openat2");
#endif
}

bool CsvReader::open_single(const std::string &allowed, const std::string &path)
{
#if defined(__linux__) && defined(SYS_openat2)
  file_ = open_regular_file(allowed, path);
  if (file_ == nullptr) {
    return fail("cannot safely open CSV as a regular file");
  }
  identity_ = current_identity();
  if (identity_.empty()) {
    close();
    return fail("cannot determine CSV file identity");
  }
  return true;
#else
  (void)allowed;
  (void)path;
  return fail("secure file opening requires Linux openat2");
#endif
}

bool CsvReader::open_glob(const std::string &allowed, const std::string &path,
                          Continue should_continue, void *context)
{
#if defined(__linux__) && defined(SYS_openat2)
  const std::size_t split = path.rfind('/');
  const std::string directory = path.substr(0, split);
  if (directory.find_first_of("*?[]") != directory.npos) {
    return fail("glob metacharacters are only allowed in the final component");
  }
  const int root = open_allowed_directory(allowed);
  if (root < 0) {
    return fail("cannot safely open glob root");
  }
  const int fd = open_beneath(root, directory == allowed ? "." : directory.c_str() + allowed.size() + 1,
                              O_RDONLY | O_DIRECTORY, RESOLVE_NO_XDEV);
  ::close(root);
  if (fd < 0) {
    return fail("cannot safely open glob directory");
  }
  DIR *entries = fdopendir(fd);
  if (entries == nullptr) {
    ::close(fd);
    return fail("cannot enumerate glob directory");
  }
  const std::string_view suffix(path.data() + split + 2, path.size() - split - 2);
  std::vector<std::string> names;
  bool ok = true;
  for (;;) {
    if (should_continue != nullptr && !should_continue(context)) {
      ok = fail("glob enumeration cancelled");
      break;
    }
    errno = 0;
    const dirent *entry = readdir(entries);
    if (entry == nullptr) {
      ok = errno == 0 || fail("glob directory read failed");
      break;
    }
    const std::string_view name(entry->d_name);
    if (name.front() != '.' && name.size() > suffix.size()
        && name.substr(name.size() - suffix.size()) == suffix) {
      if (names.size() == MAX_GLOB_FILES) {
        ok = fail("glob matches too many files");
        break;
      }
      names.emplace_back(name);
    }
  }
  closedir(entries);
  if (!ok) {
    return false;
  }
  std::sort(names.begin(), names.end());
  glob_pattern_ = path;
  glob_allowed_ = allowed;
  glob_directory_ = directory;
  identity_ = std::string(GLOB_PREFIX);
  if (format_ == Format::Glob) {
    identity_ += "files:";
  } else if (format_ == Format::Json) {
    identity_ += JSON_IDENTITY_PREFIX;
  } else if (format_ == Format::Ndjson) {
    identity_ += NDJSON_IDENTITY_PREFIX;
  } else {
    identity_ += "csv:";
  }
  for (auto &name : names) {
    if (should_continue != nullptr && !should_continue(context)) {
      return fail("glob identity validation cancelled");
    }
    CsvReader member;
    member.format_ = format_;
    if (!member.open_single(allowed, directory + "/" + name)) {
      return fail("cannot safely open glob match as a regular file");
    }
    identity_.append(name).push_back('\0');
    identity_.append(member.identity_).push_back('\0');
    glob_files_.push_back({std::move(name), std::move(member.identity_)});
  }
  return true;
#else
  (void)allowed;
  (void)path;
  (void)should_continue;
  (void)context;
  return fail("secure glob expansion requires Linux openat2");
#endif
}

bool CsvReader::open_glob_member(const GlobFile &file, CsvReader &reader)
{
  reader.format_ = format_;
  reader.glob_member_ = true;
  if (!reader.open_single(glob_allowed_, glob_directory_ + "/" + file.name)
      || reader.identity_ != file.identity) {
    return fail("glob file changed since schema inference");
  }
  return true;
}

bool CsvReader::infer_glob(std::vector<Column> &columns, Continue should_continue, void *context)
{
  if (glob_files_.empty()) {
    return fail("glob matched no files");
  }
  std::unordered_map<std::string, std::size_t> indexes;
  std::vector<std::size_t> appearances;
  for (std::size_t i = 0; i < glob_files_.size(); ++i) {
    if (should_continue != nullptr && !should_continue(context)) {
      return fail("glob schema inference cancelled");
    }
    CsvReader member;
    std::vector<Column> schema;
    if (!open_glob_member(glob_files_[i], member)) {
      return false;
    }
    if (!member.infer(schema, should_continue, context)) {
      return fail(member.error_.c_str());
    }
    for (auto &column : schema) {
      auto entry = indexes.emplace(column.name, columns.size());
      if (entry.second) {
        if (columns.size() == MAX_COLUMNS) {
          return fail("glob schema has too many columns");
        }
        appearances.push_back(1);
        columns.push_back(std::move(column));
      } else {
        ++appearances[entry.first->second];
        auto &merged = columns[entry.first->second];
        merged.nullable |= column.nullable;
        merged.double_compatible &= column.double_compatible;
        merged.scale = std::max(merged.scale, column.scale);
        if (column.type != Type::Null) {
          merged.type = merge_type(merged.type, column.type, merged.double_compatible);
        }
      }
    }
  }
  for (std::size_t i = 0; i < columns.size(); ++i) {
    columns[i].nullable |= appearances[i] != glob_files_.size();
  }
  if (columns.empty()) {
    columns.push_back({"value", Type::Null, true});
  }
  std::vector<std::string> names;
  for (const auto &column : columns) {
    names.push_back(column.name);
  }
  return start_scan(identity_, names.size(), names);
}

bool CsvReader::start_glob_member()
{
  auto reader = std::make_unique<CsvReader>();
  if (!open_glob_member(glob_files_[glob_index_], *reader)) {
    return false;
  }
  if (!reader->start_scan(glob_files_[glob_index_].identity, format_ == Format::Csv ? 0 : width_, glob_names_)) {
    return fail(reader->error_.c_str());
  }
  glob_mapping_.clear();
  for (const auto &field : reader->csv_header_) {
    const auto it = json_indexes_.find(field.value);
    if (it == json_indexes_.end()) {
      return fail("glob CSV header changed since schema inference");
    }
    glob_mapping_.push_back(it->second);
  }
  reader->csv_header_.clear();
  glob_reader_ = std::move(reader);
  return true;
}

bool CsvReader::next_glob_batch(std::size_t max_rows, std::vector<Row> &rows,
                               Continue should_continue, void *context)
{
  while (rows.size() < max_rows && glob_index_ < glob_files_.size()) {
    if (format_ == Format::Glob) {
      CsvReader member;
      if ((should_continue != nullptr && !should_continue(context))
          || !open_glob_member(glob_files_[glob_index_], member)) {
        rows.clear();
        return error_.empty() ? fail("glob scan cancelled") : false;
      }
      rows.push_back(Row{{glob_directory_ + "/" + glob_files_[glob_index_++].name, false}});
      continue;
    }
    if ((should_continue != nullptr && !should_continue(context))
        || (glob_reader_ == nullptr && !start_glob_member())) {
      rows.clear();
      return error_.empty() ? fail("glob scan cancelled") : false;
    }
    std::vector<Row> batch;
    if (!glob_reader_->next_batch(1, batch, should_continue, context)) {
      rows.clear();
      return fail(glob_reader_->error_.c_str());
    }
    if (batch.empty()) {
      glob_reader_.reset();
      ++glob_index_;
    } else if (format_ == Format::Csv) {
      Row &row = rows.emplace_back(width_, Field{{}, true});
      for (std::size_t i = 0; i < glob_mapping_.size(); ++i) {
        row[glob_mapping_[i]] = std::move(batch.front()[i]);
      }
    } else {
      rows.push_back(std::move(batch.front()));
    }
  }
  return true;
}

std::string CsvReader::current_identity() const
{
#if defined(__linux__)
  struct stat status {};
  if (file_ != nullptr && fstat(fileno(file_), &status) == 0) {
    const std::string_view prefix = format_ == Format::Parquet ? PARQUET_IDENTITY_PREFIX
        : format_ == Format::Json ? JSON_IDENTITY_PREFIX
        : format_ == Format::Ndjson ? NDJSON_IDENTITY_PREFIX : std::string_view{};
    return std::string(prefix)
        + std::to_string(status.st_dev) + ":" + std::to_string(status.st_ino) + ":"
        + std::to_string(status.st_size) + ":" + std::to_string(status.st_mtim.tv_sec) + ":"
        + std::to_string(status.st_mtim.tv_nsec) + ":" + std::to_string(status.st_ctim.tv_sec) + ":"
        + std::to_string(status.st_ctim.tv_nsec);
  }
#endif
  return {};
}

bool CsvReader::check_identity()
{
  return (!identity_.empty() && current_identity() == identity_)
      || fail(format_ == Format::Parquet ? "Parquet file changed or its identity is unavailable"
          : format_ == Format::Json ? "JSON file changed or its identity is unavailable"
          : format_ == Format::Ndjson ? "NDJSON file changed or its identity is unavailable"
                                     : "CSV file changed or its identity is unavailable");
}

CsvReader::Result CsvReader::read_record(Row &row)
{
  enum class State { Start, Bare, Quoted, Closed };
  State state = State::Start;
  bool quoted = false;
  std::string field;
  std::size_t bytes = 0;
  row.clear();
  const auto append = [&] {
    if (row.size() >= MAX_COLUMNS || !valid_utf8(field)) {
      return fail("CSV has too many columns or invalid UTF-8");
    }
    const bool is_null = !quoted && (field.empty() || field == "NULL");
    row.push_back({std::move(field), is_null});
    field.clear();
    state = State::Start;
    quoted = false;
    return true;
  };
  for (;;) {
    const int ch = std::fgetc(file_);
    if (ch == EOF) {
      if (std::ferror(file_) || state == State::Quoted) {
        fail("CSV read error or unterminated quoted field");
        return Result::Error;
      }
      if (bytes == 0) {
        return Result::Eof;
      }
      return append() ? Result::Row : Result::Error;
    }
    if (++bytes > MAX_RECORD_BYTES || ch == '\0') {
      fail("CSV record is too large or contains a NUL byte");
      return Result::Error;
    }
    if (state == State::Quoted) {
      if (ch == '"') {
        state = State::Closed;
      } else {
        field.push_back(static_cast<char>(ch));
      }
    } else if (state == State::Closed && ch == '"') {
      field.push_back('"');
      state = State::Quoted;
    } else if (ch == ',') {
      if (!append()) {
        return Result::Error;
      }
    } else if (ch == '\n' || ch == '\r') {
      if (ch == '\r' && std::fgetc(file_) != '\n') {
        fail("CSV carriage return must be followed by line feed");
        return Result::Error;
      }
      return append() ? Result::Row : Result::Error;
    } else if (state == State::Start && ch == '"') {
      state = State::Quoted;
      quoted = true;
    } else if (state == State::Closed || ch == '"') {
      fail("invalid CSV quote");
      return Result::Error;
    } else {
      state = State::Bare;
      field.push_back(static_cast<char>(ch));
    }
  }
}

CsvReader::Result CsvReader::read_object(Object &object, Continue should_continue, void *context)
{
  if (format_ == Format::Json) {
    return read_array_object(object, should_continue, context);
  }
  std::string line;
  for (;;) {
    if (should_continue != nullptr && !should_continue(context)) {
      fail("NDJSON scan cancelled");
      return Result::Error;
    }
    line.clear();
    int ch;
    while ((ch = std::fgetc(file_)) != EOF && ch != '\n') {
      if (line.size() == MAX_RECORD_BYTES || ch == '\0') {
        fail("NDJSON line is too large or contains a NUL byte");
        return Result::Error;
      }
      line.push_back(static_cast<char>(ch));
    }
    if (ch == EOF && std::ferror(file_)) {
      fail("NDJSON read error");
      return Result::Error;
    }
    if (line.find_first_not_of(" \t\r") != std::string::npos) {
      return parse_object(line, object) ? Result::Row : Result::Error;
    }
    if (ch == EOF) {
      return Result::Eof;
    }
  }
}

bool CsvReader::parse_object(const std::string &record, Object &object)
{
  object.clear();
  ObjectSax sax(object);
  rapidjson::MemoryStream input(record.data(), record.size());
  rapidjson::Reader parser;
  return (valid_utf8(record) && record.find('\0') == record.npos
      && parser.Parse<rapidjson::kParseValidateEncodingFlag | rapidjson::kParseNumbersAsStringsFlag>(input, sax))
      || fail(format_ == Format::Parquet ? "Parquet file changed or its identity is unavailable"
          : format_ == Format::Json ? "invalid JSON object: expected unique scalar fields"
          : "invalid NDJSON record: expected an object with unique scalar fields");
}

CsvReader::Result CsvReader::read_array_object(Object &object, Continue should_continue, void *context)
{
  if (array_state_ == ArrayState::Done) {
    return Result::Eof;
  }
  bool cancelled = false;
  std::size_t bytes_read = 0;
  const auto get_byte = [&]() {
    if ((bytes_read++ % 4096 == 0) && should_continue != nullptr && !should_continue(context)) {
      cancelled = true;
      return EOF;
    }
    return std::fgetc(file_);
  };
  const auto next_token = [&]() {
    int ch;
    do {
      ch = get_byte();
    } while (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r');
    return ch;
  };
  const auto invalid = [&](const char *message) {
    fail(cancelled ? "JSON scan cancelled" : std::ferror(file_) ? "JSON read error" : message);
    return Result::Error;
  };

  int ch = next_token();
  if (array_state_ == ArrayState::Start) {
    if (ch != '[') {
      return invalid("expected a top-level JSON array");
    }
    array_state_ = ArrayState::First;
    ch = next_token();
  }
  if (ch == ']') {
    if (next_token() != EOF || cancelled || std::ferror(file_)) {
      return invalid("unexpected input after JSON array");
    }
    array_state_ = ArrayState::Done;
    return Result::Eof;
  }
  if (array_state_ == ArrayState::AfterObject) {
    if (ch != ',') {
      return invalid("expected a comma or closing bracket in JSON array");
    }
    ch = next_token();
  }
  if (ch != '{') {
    return invalid("expected a JSON object in array");
  }

  std::string record(1, '{');
  std::size_t depth = 1;
  bool in_string = false;
  bool escaped = false;
  while (depth != 0) {
    ch = get_byte();
    if (ch == EOF) {
      return invalid("truncated JSON object");
    }
    if (record.size() == MAX_RECORD_BYTES) {
      return invalid("JSON object exceeds 4 MiB");
    }
    record.push_back(static_cast<char>(ch));
    if (in_string) {
      if (escaped) {
        escaped = false;
      } else if (ch == '\\') {
        escaped = true;
      } else if (ch == '"') {
        in_string = false;
      }
    } else if (ch == '"') {
      in_string = true;
    } else if (ch == '{' || ch == '[') {
      ++depth;
    } else if (ch == '}' || ch == ']') {
      --depth;
    }
  }
  array_state_ = ArrayState::AfterObject;
  return parse_object(record, object) ? Result::Row : Result::Error;
}

bool CsvReader::set_json_schema(const std::vector<std::string> &names)
{
  json_indexes_.clear();
  for (std::size_t i = 0; i < names.size(); ++i) {
    if (names[i].empty() || !json_indexes_.emplace(names[i], i).second) {
      return fail("invalid NDJSON scan schema");
    }
  }
  width_ = names.size();
  data_offset_ = 0;
  if (glob_pattern_.empty() && !rescan()) {
    data_offset_ = -1;
    return false;
  }
  return true;
}

bool CsvReader::infer_json_objects(std::vector<Column> &columns, Continue should_continue, void *context)
{
  struct ColumnState {
    Column column;
    std::size_t seen = 0;
    bool double_compatible = true;
  };
  std::vector<ColumnState> states;
  std::unordered_map<std::string, std::size_t> indexes;
  Object object;
  std::size_t records = 0;
  Result result;
  while ((result = read_object(object, should_continue, context)) == Result::Row) {
    ++records;
    for (const auto &entry : object) {
      auto inserted = indexes.emplace(entry.first, states.size());
      if (inserted.second) {
        if (states.size() == MAX_COLUMNS) {
          return fail("NDJSON schema has too many columns");
        }
        states.push_back({Column{entry.first}});
      }
      auto &state = states[inserted.first->second];
      ++state.seen;
      infer_field(state.column, entry.second, state.double_compatible);
    }
  }
  if (result == Result::Error) {
    return false;
  }
  if (states.empty() && !glob_member_) {
    if (format_ != Format::Json) {
      return fail("NDJSON file has no fields to infer");
    }
    // A fieldless array still exposes a relation for SELECT * and COUNT(*).
    states.push_back({Column{"value", Type::Null, true}});
  }
  std::vector<std::string> names;
  for (auto &state : states) {
    state.column.nullable |= state.seen != records;
    state.column.double_compatible = state.double_compatible;
    names.push_back(state.column.name);
  }
  if (!set_json_schema(names)) {
    return false;
  }
  for (auto &state : states) {
    columns.push_back(std::move(state.column));
  }
  return true;
}

CsvReader::Result CsvReader::read_json_row(Row &row, Continue should_continue, void *context)
{
  Object object;
  const Result result = read_object(object, should_continue, context);
  if (result == Result::Row) {
    row.assign(width_, Field{{}, true});
    for (auto &entry : object) {
      const auto index = json_indexes_.find(entry.first);
      if (index == json_indexes_.end()) {
        fail("NDJSON field was not present during schema inference");
        return Result::Error;
      }
      row[index->second] = std::move(entry.second);
    }
  }
  return result;
}

bool CsvReader::prepare_parquet(std::vector<Column> &columns, Continue should_continue, void *context)
{
#if defined(__linux__)
  parquet_ = std::make_unique<ParquetReader>();
  if (!parquet_->open(fileno(file_), should_continue, context) || !parquet_->infer_schema(columns)) {
    return fail(parquet_->error().c_str());
  }
  if (!check_identity()) {
    return false;
  }
  parquet_columns_ = columns;
  width_ = columns.size();
  data_offset_ = 0;
  return true;
#else
  (void)columns;
  (void)should_continue;
  (void)context;
  return fail("Parquet FILE SCAN requires Linux");
#endif
}

namespace
{
bool normalize_csv_header(const Row &header, std::vector<std::string> &names)
{
  const auto column_key = [](std::string name) {
    for (char &ch : name) {
      if (ch >= 'A' && ch <= 'Z') {
        ch += 'a' - 'A';
      }
    }
    return name;
  };
  std::unordered_set<std::string> original_names;
  std::unordered_set<std::string> used_names;
  for (const auto &field : header) {
    if (field.value.empty()) {
      return false;
    }
    original_names.insert(column_key(field.value));
  }
  for (const auto &field : header) {
    std::string name = field.value;
    if (!used_names.insert(column_key(name)).second) {
      for (size_t suffix = 2;; ++suffix) {
        name = field.value + "__" + std::to_string(suffix);
        const auto key = column_key(name);
        if (original_names.count(key) == 0 && used_names.insert(key).second) {
          break;
        }
      }
    }
    names.push_back(std::move(name));
  }
  return true;
}
} // namespace

bool CsvReader::infer(std::vector<Column> &columns, Continue should_continue, void *context)
{
  columns.clear();
  data_offset_ = -1;
  error_.clear();
  if (format_ == Format::Glob && !glob_pattern_.empty()) {
    columns.push_back({"file", Type::Text, false});
    return start_scan(identity_, 1, {"file"});
  }
  if (!glob_pattern_.empty()) {
    return infer_glob(columns, should_continue, context);
  }
  if (file_ == nullptr || std::fseek(file_, 0, SEEK_SET) != 0) {
    return fail("CSV stream is not open or seekable");
  }
  if (!check_identity()) {
    return false;
  }
  array_state_ = ArrayState::Start;
  if (format_ == Format::Parquet) {
    return prepare_parquet(columns, should_continue, context);
  }
  if (format_ != Format::Csv) {
    return infer_json_objects(columns, should_continue, context);
  }
  // Consume an optional UTF-8 BOM before parsing the first field.
  if ((std::fgetc(file_) != 0xef || std::fgetc(file_) != 0xbb || std::fgetc(file_) != 0xbf)
      && std::fseek(file_, 0, SEEK_SET) != 0) {
    return fail("cannot seek to CSV header");
  }
  Row row;
  const Result header = read_record(row);
  if (header == Result::Eof && glob_member_) {
    data_offset_ = 0;
    return rescan();
  }
  if (header != Result::Row) {
    return header == Result::Error ? false : fail("CSV file has no header");
  }
  const long offset = std::ftell(file_);
  if (offset < 0) {
    return fail("cannot determine CSV data offset");
  }

  std::vector<Column> inferred;
  std::vector<std::string> header_names;
  if (!normalize_csv_header(row, header_names)) {
    return fail("CSV header contains an empty column name");
  }
  for (auto &name : header_names) {
    inferred.push_back({std::move(name), Type::Null, false});
  }
  std::vector<bool> double_compatible(inferred.size(), true);
  Result result;
  while ((result = read_record(row)) == Result::Row) {
    if (should_continue != nullptr && !should_continue(context)) {
      return fail("CSV schema inference cancelled");
    }
    if (row.size() != inferred.size()) {
      return fail("CSV row width differs from header");
    }
    for (std::size_t i = 0; i < row.size(); ++i) {
      bool compatible = double_compatible[i];
      infer_field(inferred[i], row[i], compatible);
      if (!row[i].is_null) {
        const auto &text = row[i].value;
        const int64_t scale = text.size() > 20 && text[19] == '.' && text.size() <= 26 ? text.size() - 20 : 0;
        inferred[i].scale = std::max(inferred[i].scale, scale);
      }
      double_compatible[i] = compatible;
    }
  }
  if (result == Result::Error) {
    return false;
  }
  data_offset_ = offset;
  width_ = inferred.size();
  if (!rescan()) {
    data_offset_ = -1;
    return false;
  }
  for (std::size_t i = 0; i < inferred.size(); ++i) {
    inferred[i].double_compatible = double_compatible[i];
  }
  columns = std::move(inferred);
  return true;
}

bool CsvReader::start_scan(const std::string &expected_identity, std::size_t width,
                           const std::vector<std::string> &names,
                           Continue should_continue, void *context)
{
  error_.clear();
  data_offset_ = -1;
  if (!glob_pattern_.empty()) {
    if (identity_ != expected_identity || (glob_files_.empty() && format_ != Format::Glob)) {
      return fail("glob file set changed since schema inference");
    }
    if (width == 0 || names.size() != width || !set_json_schema(names)) {
      return fail("invalid glob scan schema");
    }
    glob_names_ = names;
    glob_reader_.reset();
    glob_index_ = 0;
    return true;
  }
  if (file_ == nullptr || identity_ != expected_identity || !check_identity()) {
    return fail(format_ == Format::Parquet ? "Parquet file changed since schema inference"
        : format_ == Format::Json ? "JSON file changed since schema inference"
        : format_ == Format::Ndjson ? "NDJSON file changed since schema inference"
                                   : "CSV file changed since schema inference");
  }
  if (format_ == Format::Parquet) {
    std::vector<Column> columns;
    return prepare_parquet(columns, should_continue, context)
        && (columns.size() == width || fail("Parquet schema width changed"));
  }
  if (format_ != Format::Csv) {
    return (width != 0 && names.size() == width) ? set_json_schema(names)
                                                 : fail("invalid JSON scan schema");
  }
  if (std::fseek(file_, 0, SEEK_SET) != 0) {
    return fail("cannot seek to CSV header");
  }
  // The unchanged file already passed header validation during binding.
  if ((std::fgetc(file_) != 0xef || std::fgetc(file_) != 0xbb || std::fgetc(file_) != 0xbf)
      && std::fseek(file_, 0, SEEK_SET) != 0) {
    return fail("cannot seek to CSV header");
  }
  Row header;
  const Result result = read_record(header);
  if (glob_member_ && result == Result::Eof) {
    data_offset_ = 0;
    return rescan();
  }
  if (result != Result::Row || (!glob_member_ && header.size() != width)) {
    return fail("CSV header changed since schema inference");
  }
  data_offset_ = std::ftell(file_);
  width_ = glob_member_ ? header.size() : width;
  if (glob_member_) {
    std::vector<std::string> names;
    if (!normalize_csv_header(header, names)) {
      return fail("CSV header contains an empty column name");
    }
    for (size_t i = 0; i < names.size(); ++i) {
      header[i].value = std::move(names[i]);
    }
    csv_header_ = std::move(header);
  }
  return rescan();
}

bool CsvReader::rescan()
{
  error_.clear();
  if (!glob_pattern_.empty()) {
    const std::string expected = identity_;
    const auto names = glob_names_;
    const auto path = glob_pattern_;
    const auto allowed = glob_allowed_;
    return open(true, allowed, path, format_) && start_scan(expected, names.size(), names);
  }
  if (parquet_ != nullptr && data_offset_ >= 0) {
    return check_identity() && (parquet_->rescan() || fail(parquet_->error().c_str()));
  }
  if (file_ == nullptr || data_offset_ < 0 || std::fseek(file_, data_offset_, SEEK_SET) != 0) {
    return fail("cannot seek to inferred file data");
  }
  array_state_ = ArrayState::Start;
  return check_identity();
}

bool CsvReader::next_batch(std::size_t max_rows, std::vector<Row> &rows,
                           Continue should_continue, void *context)
{
  error_.clear();
  rows.clear();
  if (!glob_pattern_.empty() && data_offset_ >= 0 && max_rows > 0 && max_rows <= MAX_BATCH_ROWS) {
    return next_glob_batch(max_rows, rows, should_continue, context);
  }
  if (file_ == nullptr || data_offset_ < 0 || max_rows == 0 || max_rows > MAX_BATCH_ROWS) {
    return fail("CSV batch requires an inferred stream and a valid batch size");
  }
  if (!check_identity()) {
    return false;
  }
  Row row;
  if (format_ == Format::Parquet) {
    while (rows.size() < max_rows) {
      if (!parquet_->next_row(row, should_continue, context)) {
        rows.clear();
        return fail(parquet_->error().c_str());
      }
      if (row.empty()) {
        break;
      }
      for (std::size_t i = 0; i < row.size(); ++i) {
        if (row[i].is_null) {
          continue;
        }
        auto &value = row[i].value;
        if (parquet_columns_[i].type == Type::DateTime) {
          value[10] = ' ';
          if (value.back() == 'Z') {
            value.pop_back();
          }
        } else if (parquet_columns_[i].type == Type::Boolean) {
          value = value == "1" ? "true" : "false";
        }
      }
      rows.push_back(std::move(row));
    }
    return true;
  }
  for (std::size_t i = 0; i < max_rows; ++i) {
    if (should_continue != nullptr && !should_continue(context)) {
      rows.clear();
      return fail("file scan cancelled");
    }
    const Result result = format_ != Format::Csv
        ? read_json_row(row, should_continue, context) : read_record(row);
    if (result == Result::Eof) {
      break;
    }
    if (result == Result::Error || row.size() != width_) {
      rows.clear();
      return result == Result::Error ? false : fail("CSV row width differs from header");
    }
    rows.push_back(std::move(row));
  }
  if (!check_identity()) {
    rows.clear();
    return false;
  }
  return true;
}

} // namespace oceanbase::sql::file_scan
