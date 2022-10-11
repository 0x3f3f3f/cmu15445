//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// schema.h
//
// Identification: src/include/catalog/schema.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "catalog/column.h"
#include "type/type.h"

namespace bustub {

class Schema {
 public:
  /**
   * Constructs the schema corresponding to the vector of columns, read left-to-right.
   * @param columns columns that describe the schema's individual columns
   */
  explicit Schema(const std::vector<Column> &columns);
  // 选择一个表中多个列复制，形成新的模式
  static auto CopySchema(const Schema *from, const std::vector<uint32_t> &attrs) -> Schema * {
    std::vector<Column> cols;
    // reserve是还没有创建对象，只是分了空间
    cols.reserve(attrs.size());
    for (const auto i : attrs) {
      // 结合emplace_back原地构造提高效率
      cols.emplace_back(from->columns_[i]);
    }
    return new Schema{cols};
  }

  /** @return all the columns in the schema */
  auto GetColumns() const -> const std::vector<Column> & { return columns_; }

  /**
   * Returns a specific column from the schema.
   * @param col_idx index of requested column
   * @return requested column
   */
  auto GetColumn(const uint32_t col_idx) const -> const Column & { return columns_[col_idx]; }

  /**
   * Looks up and returns the index of the first column in the schema with the specified name.
   * If multiple columns have the same name, the first such index is returned.
   * @param col_name name of column to look for
   * @return the index of a column with the given name, throws an exception if it does not exist
   */
  auto GetColIdx(const std::string &col_name) const -> uint32_t {
    for (uint32_t i = 0; i < columns_.size(); ++i) {
      if (columns_[i].GetName() == col_name) {
        return i;
      }
    }
    UNREACHABLE("Column does not exist");
  }
  // 非内联的列
  /** @return the indices of non-inlined columns */
  auto GetUnlinedColumns() const -> const std::vector<uint32_t> & { return uninlined_columns_; }
  // 列的数目
  /** @return the number of columns in the schema for the tuple */
  auto GetColumnCount() const -> uint32_t { return static_cast<uint32_t>(columns_.size()); }
  // 非内联列的数目
  /** @return the number of non-inlined columns */
  auto GetUnlinedColumnCount() const -> uint32_t { return static_cast<uint32_t>(uninlined_columns_.size()); }
  // 当前列一个元组的大小
  /** @return the number of bytes used by one tuple */
  inline auto GetLength() const -> uint32_t { return length_; }
  // 所有的列是否都是内联的
  /** @return true if all columns are inlined, false otherwise */
  inline auto IsInlined() const -> bool { return tuple_is_inlined_; }
  // 模式串的字符串表示
  /** @return string representation of this schema */
  auto ToString() const -> std::string;

 private:
  /** Fixed-length column size, i.e. the number of bytes used by one tuple. */
  uint32_t length_;

  /** All the columns in the schema, inlined and uninlined. */
  std::vector<Column> columns_;

  /** True if all the columns are inlined, false otherwise. */
  bool tuple_is_inlined_;

  /** Indices of all uninlined columns. */
  std::vector<uint32_t> uninlined_columns_;
};

}  // namespace bustub
