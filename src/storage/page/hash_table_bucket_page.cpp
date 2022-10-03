//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
  for (int i = 0; i < static_cast<int>(BUCKET_ARRAY_SIZE); ++i) {
    if (IsReadable(i) && !cmp(key, KeyAt(i))) {
      result->emplace_back(ValueAt(i));
    }
  }
  // 直接有empty()函数, 不要多此一举size() == 0函数
  // if (result->empty()) {
  //   return false;
  // }
  // return true;

  // 冗余的情况下直接转化
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  for (int i = 0; i < static_cast<int>(BUCKET_ARRAY_SIZE); ++i) {
    if (IsReadable(i) && !cmp(key, KeyAt(i)) && value == ValueAt(i)) {
      return false;
    }
  }

  for (int i = 0; i < static_cast<int>(BUCKET_ARRAY_SIZE); ++i) {
    if (!IsReadable(i)) {
      array_[i] = MappingType(key, value);
      SetOccupied(i);
      SetReadable(i);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  for (int i = 0; i < static_cast<int>(BUCKET_ARRAY_SIZE); ++i) {
    if (IsReadable(i) && !cmp(KeyAt(i), key) && ValueAt(i) == value) {
      RemoveAt(i);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  return this->array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  return this->array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  // 对应位置置反
  uint32_t idx = bucket_idx / 8;
  uint32_t mod = bucket_idx % 8;
  if ((this->readable_[idx] >> mod & 0x01) == 1) {
    this->readable_[idx] ^= (1 << mod);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  uint32_t idx = bucket_idx / 8;
  uint32_t mod = bucket_idx % 8;
  return ((this->occupied_[idx] >> mod) & 0x01);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t idx = bucket_idx / 8;
  uint32_t mod = bucket_idx % 8;
  if ((this->occupied_[idx] >> mod & 0x01) == 0) {
    this->occupied_[idx] ^= (1 << mod);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  uint32_t idx = bucket_idx / 8;
  uint32_t mod = bucket_idx % 8;
  return (this->readable_[idx] >> mod) & 0x01;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t idx = bucket_idx / 8;
  uint32_t mod = bucket_idx % 8;
  if ((this->readable_[idx] >> mod & 0x01) == 0) {
    this->readable_[idx] ^= (1 << mod);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  for (int i = 0; i < static_cast<int>(BUCKET_ARRAY_SIZE); ++i) {
    if (!IsReadable(i)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  uint32_t readable_number = 0;
  for (int i = 0; i < static_cast<int>(BUCKET_ARRAY_SIZE); ++i) {
    if (IsReadable(i)) {
      ++readable_number;
    }
  }
  return readable_number;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  return NumReadable() == 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < static_cast<size_t>(BUCKET_ARRAY_SIZE); bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::ResetData() {
  memset(occupied_, 0, sizeof occupied_);
  memset(readable_, 0, sizeof readable_);
  memset(array_, 0, sizeof(array_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetExistedData(std::vector<MappingType> *res) const -> bool {
  for (uint32_t i = 0; i < static_cast<uint32_t>(BUCKET_ARRAY_SIZE); ++i) {
    if (IsReadable(i)) {
      res->emplace_back(array_[i]);
    }
  }
  return !res->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
MappingType *HASH_TABLE_BUCKET_TYPE::GetArrayCopy() {
  uint32_t num = NumReadable();
  MappingType *copy = new MappingType[num];
  for (uint32_t i = 0, index = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      copy[index++] = array_[i];
    }
  }
  return copy;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::Reset() {
  memset(occupied_, 0, sizeof(occupied_));
  memset(readable_, 0, sizeof(readable_));
  memset(array_, 0, sizeof(array_));
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
