//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// type_id.h
//
// Identification: src/include/type/type_id.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace bustub {
enum TypeId { INVALID = 0, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, VARCHAR, TIMESTAMP };
// Every possible SQL type ID
}  // namespace bustub
