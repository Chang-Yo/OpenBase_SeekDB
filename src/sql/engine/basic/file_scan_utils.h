/* Copyright (c) 2026 OceanBase. Licensed under Apache License 2.0. */
#pragma once

#include <cstdio>
#include <cstdint>
#include <string>
#include <vector>
#include <sys/stat.h>

namespace oceanbase { namespace sql { namespace file_sql {
// The reader owns at most one bounded record. Schema inference never retains rows.
constexpr size_t MAX_RECORD_BYTES = 16 * 1024 * 1024;
constexpr size_t MAX_COLUMNS = 4096;
enum class Kind { Null, Boolean, Int64, Double, Date, DateTime, String };
struct Cell {
  std::string text;
  bool is_null = false;
  bool native = false;
  int64_t integer = 0;
  double floating = 0;
};
struct Column {
  std::string name;
  Kind kind = Kind::Null;
  bool nullable = false;
};
struct CsvOptions {
  bool discover = false;
  bool json_lines = false;
  bool parquet = false;
  bool header = true;
  char delimiter = ',';
  char quote = '"';
  char escape = '"';
  std::string nullstr = "NULL";
};
struct FileIdentity {
  uint64_t device = 0, inode = 0, size = 0;
  int64_t mtime_sec = 0, mtime_nsec = 0, ctime_sec = 0, ctime_nsec = 0;
  static FileIdentity from_stat(const struct stat &s);
  bool operator==(const FileIdentity &other) const;
};
Kind infer_kind(const std::string &text);
Kind widen(Kind left, Kind right);
const char *kind_name(Kind kind);

class CsvReader {
public:
  CsvReader() = default;
  ~CsvReader() { close(); }
  CsvReader(const CsvReader &) = delete;
  CsvReader &operator=(const CsvReader &) = delete;
  // All failures include a stable diagnostic; EOF is separate from failure.
  bool open(const std::string &path, const std::string &secure_root,
            const CsvOptions &options, std::string &error);
  bool next(std::vector<Cell> &row, bool &eof, std::string &error);
  bool rewind(std::string &error);
  bool unchanged(std::string &error) const;
  bool infer(std::vector<Column> &columns, uint64_t &rows, std::string &error);
  void set_parquet_projection(const std::vector<uint32_t> &indices) { parquet_projection_ = indices; }
  void set_json_columns(const std::vector<std::string> &names) { json_names_ = names; }
  void close();
  const FileIdentity &identity() const { return identity_; }
  const std::string &path() const { return path_; }
private:
  std::vector<std::string> discovered_;
  size_t discovered_pos_ = 0;
  bool discover(const std::string &pattern, const std::string &root, std::string &error);
  bool next_json(std::vector<Cell> &row, bool &eof, std::string &error);
  bool infer_json(std::vector<Column> &columns, uint64_t &rows, std::string &error);
  std::vector<std::string> json_names_;
  std::vector<Kind> json_kinds_;
  bool infer_json_ = false;
  bool fail(std::string &error, const char *message) const;
  FILE *file_ = nullptr;
  void *parquet_ = nullptr;
  std::vector<uint32_t> parquet_projection_;
  size_t parquet_width_ = 0;
  CsvOptions options_;
  FileIdentity identity_;
  std::string path_;
  uint64_t line_ = 1;
};
// Local COPY sink: temporary data becomes visible only after successful completion.
class CsvWriter {
public:
  CsvWriter() = default;
  ~CsvWriter() { abort(); }
  CsvWriter(const CsvWriter &) = delete;
  CsvWriter &operator=(const CsvWriter &) = delete;
  bool open(const std::string &path, const std::string &root,
            const CsvOptions &options, std::string &error);
  bool write(const std::vector<Cell> &row, std::string &error);
  bool commit(std::string &error);
  void abort();
private:
  FILE *file_ = nullptr;
  int directory_ = -1;
  std::string target_, temporary_;
  CsvOptions options_;
};
}}}
