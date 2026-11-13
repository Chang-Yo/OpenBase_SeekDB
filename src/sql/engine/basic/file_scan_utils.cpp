/* Copyright (c) 2026 OceanBase. Licensed under Apache License 2.0. */
#include "file_scan_utils.h"
#include "file_sql_bridge.h"
#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <climits>
#include <fcntl.h>
#include <unistd.h>
#include <unordered_set>
#include <dirent.h>
#include <fnmatch.h>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

namespace oceanbase { namespace sql { namespace file_sql {
FileIdentity FileIdentity::from_stat(const struct stat &s)
{
  FileIdentity v;
  v.device = s.st_dev; v.inode = s.st_ino; v.size = s.st_size;
#if defined(__APPLE__)
  v.mtime_sec = s.st_mtimespec.tv_sec; v.mtime_nsec = s.st_mtimespec.tv_nsec;
  v.ctime_sec = s.st_ctimespec.tv_sec; v.ctime_nsec = s.st_ctimespec.tv_nsec;
#else
  v.mtime_sec = s.st_mtim.tv_sec; v.mtime_nsec = s.st_mtim.tv_nsec;
  v.ctime_sec = s.st_ctim.tv_sec; v.ctime_nsec = s.st_ctim.tv_nsec;
#endif
  return v;
}
bool FileIdentity::operator==(const FileIdentity &v) const
{
  return device == v.device && inode == v.inode && size == v.size
      && mtime_sec == v.mtime_sec && mtime_nsec == v.mtime_nsec
      && ctime_sec == v.ctime_sec && ctime_nsec == v.ctime_nsec;
}
static bool digits(const std::string &s, size_t begin, size_t end)
{
  if (end > s.size() || begin == end) return false;
  for (size_t i = begin; i < end; ++i) if (s[i] < '0' || s[i] > '9') return false;
  return true;
}
static bool date(const std::string &s)
{
  if (s.size() < 10 || s[4] != '-' || s[7] != '-' || !digits(s,0,4)
      || !digits(s,5,7) || !digits(s,8,10)) return false;
  int y = std::atoi(s.substr(0,4).c_str()), m = std::atoi(s.substr(5,2).c_str());
  int d = std::atoi(s.substr(8,2).c_str());
  static const int days[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
  return y > 0 && m >= 1 && m <= 12 && d > 0
      && d <= days[m] + (m == 2 && y % 4 == 0 && (y % 100 != 0 || y % 400 == 0));
}
Kind infer_kind(const std::string &s)
{
  if (s == "true" || s == "false" || s == "TRUE" || s == "FALSE") return Kind::Boolean;
  if (date(s)) {
    if (s.size() == 10) return Kind::Date;
    if (s.size() >= 19 && (s[10] == 'T' || s[10] == ' ') && s[13] == ':' && s[16] == ':'
        && digits(s,11,13) && digits(s,14,16) && digits(s,17,19)
        && std::atoi(s.substr(11,2).c_str()) < 24 && std::atoi(s.substr(14,2).c_str()) < 60
        && std::atoi(s.substr(17,2).c_str()) < 60
        && (s.size() == 19 || (s.size() <= 26 && s[19] == '.' && digits(s,20,s.size()))))
      return Kind::DateTime;
  }
  const size_t start = !s.empty() && (s[0] == '+' || s[0] == '-') ? 1 : 0;
  if (digits(s,start,s.size())) {
    errno = 0; char *end = nullptr; std::strtoll(s.c_str(), &end, 10);
    // Preserve integers outside signed BIGINT exactly as VARCHAR, never round to double.
    return errno == ERANGE ? Kind::String : Kind::Int64;
  }
  // Require decimal SQL-like syntax; libc also accepts hex and non-finite values.
  size_t i = start; bool any = false;
  while (i < s.size() && s[i] >= '0' && s[i] <= '9') { ++i; any = true; }
  if (i < s.size() && s[i] == '.') {
    ++i;
    while (i < s.size() && s[i] >= '0' && s[i] <= '9') { ++i; any = true; }
  }
  if (any && i < s.size() && (s[i] == 'e' || s[i] == 'E')) {
    ++i; if (i < s.size() && (s[i] == '+' || s[i] == '-')) ++i;
    if (!digits(s,i,s.size())) return Kind::String;
    i = s.size();
  }
  if (any && i == s.size()) {
    errno = 0; char *end = nullptr; double value = std::strtod(s.c_str(), &end);
    if (errno != ERANGE && std::isfinite(value) && end == s.c_str()+s.size()) return Kind::Double;
  }
  return Kind::String;
}
Kind widen(Kind a, Kind b)
{
  if (a == Kind::Null) return b;
  if (b == Kind::Null || a == b) return a;
  if ((a == Kind::Int64 && b == Kind::Double) || (b == Kind::Int64 && a == Kind::Double)) return Kind::Double;
  if ((a == Kind::Date && b == Kind::DateTime) || (b == Kind::Date && a == Kind::DateTime)) return Kind::DateTime;
  return Kind::String;
}
const char *kind_name(Kind k)
{
  switch (k) {
    case Kind::Boolean: return "BOOLEAN"; case Kind::Int64: return "BIGINT";
    case Kind::Double: return "DOUBLE"; case Kind::Date: return "DATE";
    case Kind::DateTime: return "DATETIME"; default: return "VARCHAR";
  }
}
bool CsvReader::fail(std::string &error, const char *message) const
{
  error = path_ + ": line " + std::to_string(line_) + ": " + message;
  return false;
}
void CsvReader::close()
{
  if (parquet_) { auto owned = rust::Box<parquet_io::ParquetScan>::from_raw(static_cast<parquet_io::ParquetScan *>(parquet_)); parquet_ = nullptr; }
  if (file_) std::fclose(file_);
  file_ = nullptr;
}
bool CsvReader::open(const std::string &path, const std::string &secure_root,
                     const CsvOptions &options, std::string &error)
{
  close(); path_ = path; line_ = 1; options_ = options;
  if (options.discover) return discover(path,secure_root,error);
  if (!options.delimiter || !options.quote || !options.escape || options.delimiter == options.quote
      || options.delimiter == '\n' || options.delimiter == '\r' || options.quote == '\n'
      || options.quote == '\r' || options.escape == '\n' || options.escape == '\r')
    return fail(error,"invalid CSV delimiter, quote or escape");
  if (path.find('\0') != std::string::npos || secure_root.find('\0') != std::string::npos
      || secure_root.empty()) return fail(error,"secure_file_priv denies path");
  char root[PATH_MAX], resolved[PATH_MAX]; struct stat root_stat;
  if (!::realpath(secure_root.c_str(),root) || ::stat(root,&root_stat) || !S_ISDIR(root_stat.st_mode)
      || !::realpath(path.c_str(),resolved)) return fail(error,"cannot resolve permitted file path");
  const std::string allowed(root), canonical(resolved);
  if (allowed != "/" && (canonical.size() <= allowed.size()
      || canonical.compare(0,allowed.size(),allowed) || canonical[allowed.size()] != '/'))
    return fail(error,"path is outside secure_file_priv");
  // Walk the canonical path with no-follow directory descriptors. A concurrent
  // symlink replacement cannot redirect an intermediate component outside root.
  int dir = ::open("/",O_RDONLY|O_DIRECTORY|O_CLOEXEC);
  if (dir < 0) return fail(error,"cannot open root directory");
  size_t begin = 1, slash; int fd = -1;
  while ((slash = canonical.find('/',begin)) != std::string::npos) {
    int child = ::openat(dir,canonical.substr(begin,slash-begin).c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
    ::close(dir); dir = child;
    if (dir < 0) return fail(error,"file path changed during open");
    begin = slash + 1;
  }
  fd = ::openat(dir,canonical.substr(begin).c_str(),O_RDONLY|O_NOFOLLOW|O_NONBLOCK|O_CLOEXEC);
  ::close(dir);
  struct stat st;
  if (fd < 0) return fail(error,"cannot open file");
  if (::fstat(fd,&st) || !S_ISREG(st.st_mode)) {
    ::close(fd); return fail(error,"input must be a regular file");
  }
  file_ = ::fdopen(fd,"rb");
  if (!file_) { ::close(fd); return fail(error,"cannot create file stream"); }
  path_ = canonical; identity_ = FileIdentity::from_stat(st);
  return rewind(error);
}
bool CsvReader::unchanged(std::string &error) const
{
  if (options_.discover) return true;
  struct stat descriptor, pathname;
  if (!file_ || ::fstat(::fileno(file_),&descriptor) || ::stat(path_.c_str(),&pathname)
      || !(identity_ == FileIdentity::from_stat(descriptor))
      || !(identity_ == FileIdentity::from_stat(pathname))) return fail(error,"file changed during query");
  return true;
}
bool CsvReader::rewind(std::string &error)
{
  if (options_.discover) { discovered_pos_ = 0; return true; }
  if (!unchanged(error)) return false;
  if (::fseeko(file_,0,SEEK_SET)) return fail(error,"cannot rewind file");
  line_ = 1;
  if (options_.parquet) {
    if (parquet_) { auto owned = rust::Box<parquet_io::ParquetScan>::from_raw(static_cast<parquet_io::ParquetScan *>(parquet_)); parquet_ = nullptr; }
    auto scan = parquet_io::open_checked(::fileno(file_),rust::Slice<const uint32_t>(parquet_projection_.data(),parquet_projection_.size()));
    auto message = scan->error();
    if (!message.empty()) { error.assign(message.data(),message.size()); return false; }
    parquet_width_ = scan->columns().size();
    parquet_ = scan.into_raw();
    return unchanged(error);
  }
  unsigned char bom[3]; size_t n = std::fread(bom,1,3,file_);
  if (!(n == 3 && bom[0] == 0xef && bom[1] == 0xbb && bom[2] == 0xbf)) {
    if (::fseeko(file_,0,SEEK_SET)) return fail(error,"cannot rewind file");
  }
  return true;
}
static bool valid_utf8(const std::string &s)
{
  size_t i = 0;
  while (i < s.size()) {
    unsigned char c = s[i++];
    if (c < 0x80) continue;
    unsigned remaining = 0; uint32_t cp = 0, minimum = 0;
    if (c >= 0xc2 && c <= 0xdf) { remaining = 1; cp = c & 0x1f; minimum = 0x80; }
    else if (c >= 0xe0 && c <= 0xef) { remaining = 2; cp = c & 0xf; minimum = 0x800; }
    else if (c >= 0xf0 && c <= 0xf4) { remaining = 3; cp = c & 7; minimum = 0x10000; }
    else return false;
    if (remaining > s.size() - i) return false;
    while (remaining--) {
      c = s[i++]; if ((c & 0xc0) != 0x80) return false;
      cp = (cp << 6) | (c & 0x3f);
    }
    if (cp < minimum || cp > 0x10ffff || (cp >= 0xd800 && cp <= 0xdfff)) return false;
  }
  return true;
}
bool CsvReader::next(std::vector<Cell> &row, bool &eof, std::string &error)
{
  if (options_.discover) {
    row.clear(); eof = discovered_pos_ >= discovered_.size();
    if (!eof) { Cell cell; cell.text = discovered_[discovered_pos_++]; row.push_back(std::move(cell)); }
    return true;
  }
  if (options_.parquet) {
    if (!parquet_ || !unchanged(error)) return false;
    auto value = static_cast<parquet_io::ParquetScan *>(parquet_)->read_checked();
    if (!value.error.empty()) { error.assign(value.error.data(),value.error.size()); return false; }
    eof = value.eof; row.clear();
    if (eof) return unchanged(error);
    row.resize(parquet_width_);
    for (auto &cell : row) cell.is_null = true;
    const size_t expected = parquet_projection_.empty() ? parquet_width_ : parquet_projection_.size();
    if (value.cells.size() != expected) return fail(error,"Parquet projection width mismatch");
    size_t index = 0;
    for (const auto &v : value.cells) {
      Cell c; c.is_null = v.null; c.native = true; c.integer = v.integer; c.floating = v.floating;
      if (!v.bytes.empty()) c.text.assign(reinterpret_cast<const char *>(v.bytes.data()),v.bytes.size());
      const size_t target = parquet_projection_.empty() ? index : parquet_projection_[index];
      if (target >= row.size()) return fail(error,"invalid Parquet projected index");
      row[target] = std::move(c); ++index;
    }
    return unchanged(error);
  }
  if (options_.json_lines) return next_json(row,eof,error);
  row.clear(); eof = false;
  if (!file_) return fail(error,"reader is closed");
  enum State { Start, Plain, Quoted, AfterQuote }; State state = Start;
  Cell cell; bool quoted = false, seen = false; size_t bytes = 0;
  auto append = [&]() {
    cell.is_null = !quoted && (cell.text.empty() || cell.text == options_.nullstr);
    row.push_back(std::move(cell)); cell = Cell(); quoted = false; state = Start;
  };
  for (;;) {
    int c = std::fgetc(file_);
    if (c == EOF) {
      if (std::ferror(file_)) return fail(error,"file read failed");
      if (state == Quoted) return fail(error,"unterminated quoted field");
      if (!unchanged(error)) return false;
      if (!seen) { eof = true; return true; }
      append();
      for (const auto &v : row) if (!valid_utf8(v.text)) return fail(error,"invalid UTF-8 CSV field");
      return true;
    }
    seen = true;
    if (++bytes > MAX_RECORD_BYTES) return fail(error,"record exceeds 16 MiB limit");
    if (c == 0) return fail(error,"NUL byte in CSV input");
    if (state == Quoted) {
      if (c == options_.quote) state = AfterQuote;
      else if (c == options_.escape && options_.escape != options_.quote) {
        int escaped = std::fgetc(file_);
        if (escaped != options_.escape && escaped != options_.quote) return fail(error,"invalid quoted escape");
        if (++bytes > MAX_RECORD_BYTES) return fail(error,"record exceeds 16 MiB limit");
        cell.text.push_back(static_cast<char>(escaped));
      } else {
        if (c == '\n') ++line_;
        cell.text.push_back(static_cast<char>(c));
      }
    } else if (state == AfterQuote && c == options_.quote && options_.escape == options_.quote) {
      cell.text.push_back(static_cast<char>(c)); state = Quoted;
    } else if (c == options_.delimiter) {
      append();
      if (row.size() >= MAX_COLUMNS) return fail(error,"column count exceeds 4096 limit");
    } else if (c == '\n' || c == '\r') {
      if (c == '\r') { int n = std::fgetc(file_); if (n != '\n' && n != EOF) std::ungetc(n,file_); }
      ++line_; append();
      for (const auto &v : row) if (!valid_utf8(v.text)) return fail(error,"invalid UTF-8 CSV field");
      return true;
    } else if (state == Start && c == options_.quote) {
      state = Quoted; quoted = true;
    } else if (state == AfterQuote || c == options_.quote) {
      return fail(error,"unexpected character or quote in CSV field");
    } else {
      state = Plain; cell.text.push_back(static_cast<char>(c));
    }
  }
}
bool CsvReader::infer(std::vector<Column> &columns, uint64_t &rows, std::string &error)
{
  if (options_.discover) {
    Column column; column.name = "file"; column.kind = Kind::String;
    columns.assign(1,column); rows = discovered_.size(); discovered_pos_ = 0; return true;
  }
  if (options_.parquet) {
    if (!parquet_ || !unchanged(error)) return false;
    auto &scan = *static_cast<parquet_io::ParquetScan *>(parquet_);
    columns.clear(); rows = scan.row_count(); std::unordered_set<std::string> names;
    for (const auto &v : scan.columns()) {
      Column c; c.name.assign(v.name.data(),v.name.size()); c.nullable = v.nullable;
      std::string folded = c.name; for (char &ch : folded) if (ch >= 'A' && ch <= 'Z') ch += 'a'-'A';
      if (c.name.empty() || !names.insert(folded).second) return fail(error,"empty or ambiguous Parquet column name");
      c.kind = static_cast<Kind>(v.kind); columns.push_back(std::move(c));
    }
    return true;
  }
  if (options_.json_lines) return infer_json(columns,rows,error);
  columns.clear(); rows = 0;
  if (!rewind(error)) return false;
  std::vector<Cell> row; bool eof = false;
  if (!next(row,eof,error)) return false;
  if (eof) return fail(error,"empty CSV has no inferable schema");
  columns.resize(row.size()); std::unordered_set<std::string> names;
  for (size_t i=0; i<columns.size(); ++i) {
    std::string base = options_.header && !row[i].text.empty() ? row[i].text : "column"+std::to_string(i);
    std::string name = base; size_t suffix = 2;
    auto folded = [](std::string s) { for (char &c:s) if (c >= 'A' && c <= 'Z') c += 'a'-'A'; return s; };
    while (!names.insert(folded(name)).second) name = base+"__"+std::to_string(suffix++);
    columns[i].name = name;
  }
  bool first = true;
  for (;;) {
    if (!first || options_.header) { if (!next(row,eof,error)) return false; }
    first = false; if (eof) break;
    if (row.size() != columns.size()) return fail(error,"record width differs from schema");
    ++rows;
    for (size_t i=0; i<row.size(); ++i) {
      columns[i].nullable |= row[i].is_null;
      if (!row[i].is_null) columns[i].kind = widen(columns[i].kind,infer_kind(row[i].text));
    }
  }
  if (!rewind(error)) return false;
  if (options_.header && !next(row,eof,error)) return false;
  return true;
}
bool CsvReader::next_json(std::vector<Cell> &row, bool &eof, std::string &error)
{
  row.clear(); eof = false;
  if (!file_) return fail(error,"reader is closed");
  std::string line;
  for (;;) {
    int c = std::fgetc(file_);
    if (c == EOF) {
      if (std::ferror(file_)) return fail(error,"file read failed");
      if (!unchanged(error)) return false;
      if (line.empty()) { eof = true; return true; }
      break;
    }
    if (c == '\n') {
      ++line_;
      if (line.find_first_not_of(" \t\r") == std::string::npos) { line.clear(); continue; }
      break;
    }
    if (line.size() >= MAX_RECORD_BYTES) return fail(error,"JSONL record exceeds 16 MiB limit");
    line.push_back(static_cast<char>(c));
  }
  rapidjson::Document doc;
  doc.Parse<rapidjson::kParseValidateEncodingFlag | rapidjson::kParseFullPrecisionFlag>(line.data(),line.size());
  if (doc.HasParseError() || !doc.IsObject()) return fail(error,"JSONL record must be a valid UTF-8 flat object");
  if (doc.MemberCount() > MAX_COLUMNS) return fail(error,"JSONL column count exceeds 4096 limit");
  row.resize(json_names_.size());
  for (auto &cell : row) cell.is_null = true;
  json_kinds_.assign(json_names_.size(),Kind::Null);
  std::unordered_set<std::string> seen;
  for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
    std::string name(it->name.GetString(),it->name.GetStringLength());
    if (name.empty() || name.find('\0') != std::string::npos || !seen.insert(name).second)
      return fail(error,"JSONL empty, duplicate or NUL field name");
    auto fold = [](std::string value) {
      for (char &c : value) if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
      return value;
    };
    for (const auto &known : json_names_) {
      if (known != name && fold(known) == fold(name))
        return fail(error,"JSONL fields differ only in case and are ambiguous in SQL");
    }
    auto pos = std::find(json_names_.begin(),json_names_.end(),name);
    size_t index = pos - json_names_.begin();
    if (pos == json_names_.end()) {
      if (!infer_json_) return fail(error,"JSONL schema changed during scan");
      if (json_names_.size() >= MAX_COLUMNS) return fail(error,"JSONL schema exceeds 4096 columns");
      json_names_.push_back(name); row.emplace_back(); json_kinds_.push_back(Kind::Null);
    }
    const auto &value = it->value;
    Cell &cell = row[index];
    cell.is_null = value.IsNull();
    if (value.IsObject() || value.IsArray()) return fail(error,"nested JSON objects and arrays are not supported");
    if (cell.is_null) continue;
    if (value.IsString()) {
      cell.text.assign(value.GetString(),value.GetStringLength());
      Kind k = infer_kind(cell.text);
      json_kinds_[index] = k == Kind::Date || k == Kind::DateTime ? k : Kind::String;
    } else if (value.IsBool()) {
      cell.text = value.GetBool() ? "true" : "false"; json_kinds_[index] = Kind::Boolean;
    } else {
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer); value.Accept(writer);
      cell.text.assign(buffer.GetString(),buffer.GetSize());
      json_kinds_[index] = value.IsInt64() ? Kind::Int64 : value.IsUint64() ? Kind::String : Kind::Double;
    }
  }
  return true;
}
bool CsvReader::infer_json(std::vector<Column> &columns, uint64_t &rows, std::string &error)
{
  columns.clear(); json_names_.clear(); rows = 0;
  if (!rewind(error)) return false;
  infer_json_ = true;
  std::vector<Cell> row; bool eof = false;
  for (;;) {
    if (!next_json(row,eof,error)) { infer_json_ = false; return false; }
    if (eof) break;
    const size_t old = columns.size(); columns.resize(json_names_.size());
    for (size_t i = old; i < columns.size(); ++i) {
      columns[i].name = json_names_[i]; columns[i].nullable = rows > 0;
    }
    for (size_t i = 0; i < columns.size(); ++i) {
      columns[i].nullable |= row[i].is_null;
      columns[i].kind = widen(columns[i].kind,json_kinds_[i]);
    }
    ++rows;
  }
  infer_json_ = false;
  if (columns.empty()) return fail(error,"JSONL has no inferable fields");
  return rewind(error);
}
bool CsvReader::discover(const std::string &pattern, const std::string &root, std::string &error)
{
  discovered_.clear(); discovered_pos_ = 0; identity_ = FileIdentity();
  char resolved[PATH_MAX];
  if (root.empty() || root.find('\0') != std::string::npos || !::realpath(root.c_str(),resolved))
    return fail(error,"invalid secure_file_priv directory");
  std::string allowed(resolved);
  if (pattern.empty() || pattern[0] != '/' || pattern.find('\0') != std::string::npos)
    return fail(error,"glob requires an absolute local pattern");
  std::vector<std::string> parts;
  size_t begin = 1;
  while (begin < pattern.size()) {
    size_t end = pattern.find('/',begin); if (end == std::string::npos) end = pattern.size();
    std::string part = pattern.substr(begin,end-begin);
    if (part == "..") return fail(error,"glob parent traversal is not allowed");
    if (!part.empty() && part != ".") parts.push_back(part);
    begin = end + 1;
  }
  if (parts.empty()) return fail(error,"glob pattern must name a file");
  // Require the fixed directory prefix to resolve inside the permitted root.
  // Wildcards may expand below this prefix, never in ancestors of the root.
  std::string prefix = "/";
  size_t fixed = 0;
  for (; fixed + 1 < parts.size(); ++fixed) {
    if (parts[fixed].find_first_of("*?[") != std::string::npos) break;
    prefix += (prefix == "/" ? "" : "/") + parts[fixed];
  }
  if (!::realpath(prefix.c_str(),resolved)) return fail(error,"cannot resolve glob directory");
  prefix = resolved;
  if (allowed != "/" && prefix != allowed
      && !(prefix.size() > allowed.size() && prefix.compare(0,allowed.size(),allowed) == 0
           && prefix[allowed.size()] == '/')) return fail(error,"glob is outside secure_file_priv");
  parts.erase(parts.begin(),parts.begin()+fixed);
  std::vector<std::string> paths(1,prefix);
  size_t visited = 0;
  for (size_t depth = 0; depth < parts.size(); ++depth) {
    std::vector<std::string> next;
    for (const auto &parent : paths) {
      int fd = ::open("/",O_RDONLY|O_DIRECTORY|O_CLOEXEC);
      size_t offset = 1;
      while (fd >= 0 && offset < parent.size()) {
        size_t end = parent.find('/',offset);
        if (end == std::string::npos) end = parent.size();
        int child = ::openat(fd,parent.substr(offset,end-offset).c_str(),
                             O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
        ::close(fd); fd = child; offset = end + 1;
      }
      if (fd < 0) continue;
      DIR *dir = ::fdopendir(fd);
      if (!dir) { ::close(fd); return fail(error,"cannot enumerate glob directory"); }
      struct dirent *entry;
      while ((entry = ::readdir(dir))) {
        if (++visited > 1000000) { ::closedir(dir); return fail(error,"glob traversal exceeds one million entries"); }
        if (::fnmatch(parts[depth].c_str(),entry->d_name,FNM_PERIOD) != 0) continue;
        std::string candidate = parent + (parent == "/" ? "" : "/") + entry->d_name;
        struct stat st;
        if (::fstatat(fd,entry->d_name,&st,AT_SYMLINK_NOFOLLOW)) continue;
        // Never follow symlinks during discovery; reading functions validate them independently.
        if (S_ISLNK(st.st_mode)) continue;
        bool inside = allowed == "/" || candidate == allowed
          || (candidate.size() > allowed.size() && candidate.compare(0,allowed.size(),allowed) == 0 && candidate[allowed.size()] == '/');
        bool ancestor = candidate == "/" || (allowed.size() > candidate.size()
          && allowed.compare(0,candidate.size(),candidate) == 0 && allowed[candidate.size()] == '/');
        if (!inside && !ancestor) continue;
        if (depth + 1 == parts.size()) {
          if (!inside) { ::closedir(dir); return fail(error,"glob is outside secure_file_priv"); }
          if (S_ISREG(st.st_mode)) next.push_back(candidate);
        } else if (S_ISDIR(st.st_mode)) next.push_back(candidate);
        if (next.size() > 100000) { ::closedir(dir); return fail(error,"glob exceeds 100000 paths"); }
      }
      ::closedir(dir);
    }
    paths.swap(next);
  }
  discovered_.swap(paths);
  std::sort(discovered_.begin(),discovered_.end());
  return true;
}
void CsvWriter::abort()
{
  if (file_) { std::fclose(file_); file_ = nullptr; }
  if (directory_ >= 0) {
    if (!temporary_.empty()) ::unlinkat(directory_,temporary_.c_str(),0);
    ::close(directory_); directory_ = -1;
  }
  temporary_.clear();
}
bool CsvWriter::open(const std::string &path, const std::string &root,
                     const CsvOptions &options, std::string &error)
{
  abort(); options_ = options;
  auto fail = [&](const char *message) { error = message; abort(); return false; };
  if (path.empty() || path.find('\0') != std::string::npos || root.empty()
      || root.find('\0') != std::string::npos) return fail("invalid COPY path");
  if (!options.delimiter || !options.quote || !options.escape || options.delimiter == options.quote
      || options.delimiter == '\r' || options.delimiter == '\n' || options.quote == '\r'
      || options.quote == '\n' || options.escape == '\r' || options.escape == '\n')
    return fail("invalid COPY CSV options");
  if (options.nullstr.find_first_of(std::string()+options.delimiter+options.quote+"\r\n") != std::string::npos
      || options.nullstr.find('\0') != std::string::npos) return fail("COPY NULL token cannot contain CSV syntax");
  size_t slash = path.rfind('/');
  if (slash == std::string::npos) return fail("COPY requires an absolute output path");
  target_ = path.substr(slash+1);
  if (target_.empty() || target_ == "." || target_ == "..") return fail("invalid COPY filename");
  char parent_buf[PATH_MAX], root_buf[PATH_MAX];
  if (!::realpath(path.substr(0,slash ? slash : 1).c_str(),parent_buf)
      || !::realpath(root.c_str(),root_buf)) return fail("cannot resolve COPY directory");
  std::string parent(parent_buf), allowed(root_buf);
  if (allowed != "/" && parent != allowed && !(parent.size() > allowed.size()
      && parent.compare(0,allowed.size(),allowed) == 0 && parent[allowed.size()] == '/'))
    return fail("COPY path is outside secure_file_priv");
  directory_ = ::open("/",O_RDONLY|O_DIRECTORY|O_CLOEXEC);
  size_t offset = 1;
  while (directory_ >= 0 && offset < parent.size()) {
    size_t end = parent.find('/',offset); if (end == std::string::npos) end = parent.size();
    int child = ::openat(directory_,parent.substr(offset,end-offset).c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
    ::close(directory_); directory_ = child; offset = end+1;
  }
  if (directory_ < 0) return fail("cannot open COPY directory");
  struct stat st;
  if (::fstatat(directory_,target_.c_str(),&st,AT_SYMLINK_NOFOLLOW) == 0 || errno != ENOENT)
    return fail("COPY target already exists or cannot be checked");
  int fd = -1;
  for (unsigned attempt = 0; attempt < 128 && fd < 0; ++attempt) {
    unsigned long long nonce = 0;
    FILE *random = std::fopen("/dev/urandom","rb");
    if (!random) return fail("cannot generate COPY temporary filename");
    const bool read = std::fread(&nonce,sizeof(nonce),1,random) == 1;
    std::fclose(random);
    if (!read) return fail("cannot generate COPY temporary filename");
    temporary_ = ".seekdb-copy-"+std::to_string(nonce);
    fd = ::openat(directory_,temporary_.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600);
    if (fd < 0) { temporary_.clear(); if (errno != EEXIST) return fail("cannot create COPY temporary file"); }
  }
  if (fd < 0) return fail("cannot allocate COPY temporary filename");
  file_ = ::fdopen(fd,"wb");
  if (!file_) { ::close(fd); return fail("cannot create COPY stream"); }
  return true;
}
bool CsvWriter::write(const std::vector<Cell> &row, std::string &error)
{
  if (!file_) { error = "COPY writer is closed"; return false; }
  std::string record;
  for (size_t i = 0; i < row.size(); ++i) {
    if (i) record += options_.delimiter;
    const auto &cell = row[i];
    if (cell.is_null) record += options_.nullstr;
    else {
      // Quote every non-NULL field, preserving empty strings and literal NULL tokens.
      record += options_.quote;
      for (char c : cell.text) {
        if (c == options_.quote || (c == options_.escape && options_.escape != options_.quote)) record += options_.escape;
        record += c;
        if (record.size() > MAX_RECORD_BYTES) { error = "COPY record exceeds 16 MiB limit"; return false; }
      }
      record += options_.quote;
    }
  }
  record += '\n';
  if (std::fwrite(record.data(),1,record.size(),file_) != record.size()) { error = "COPY write failed"; return false; }
  return true;
}
bool CsvWriter::commit(std::string &error)
{
  if (!file_) { error = "COPY writer is closed"; return false; }
  if (std::fflush(file_) || ::fsync(::fileno(file_))) { error = "COPY flush failed"; abort(); return false; }
  int result = std::fclose(file_); file_ = nullptr;
  if (result) { error = "COPY close failed"; abort(); return false; }
  // linkat atomically publishes without replacing a concurrently created target.
  if (::linkat(directory_,temporary_.c_str(),directory_,target_.c_str(),0)) {
    error = "COPY target exists or publication failed"; abort(); return false;
  }
  ::unlinkat(directory_,temporary_.c_str(),0);
  temporary_.clear(); ::close(directory_); directory_ = -1;
  return true;
}
}}}
