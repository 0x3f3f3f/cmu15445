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
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  u_int8_t mask = 255;
  // 先以char为单位
  size_t i_num = BUCKET_ARRAY_SIZE / 8;
  for (size_t i = 0; i < i_num; i++) {
    uint8_t c = static_cast<uint8_t>(readable_[i]);
    if ((c & mask) != mask) {
      return false;
    }
  }

  // 最后还要看剩余的
  size_t i_remain = BUCKET_ARRAY_SIZE % 8;
  if (i_remain > 0) {
    uint8_t c = static_cast<uint8_t>(readable_[i_num]);
    for (size_t j = 0; j < i_remain; j++) {
      if ((c & 1) != 1) {
        return false;
      }
      c >>= 1;
    }
  }
  return true;
}

// 自己在这两个函数都写错，BUCKET_ARRAY_SIZE有可能占用的char不是整数个，也就是最后一个char，可能没有占用八位
// BUCKET_ARRAY_SIZE会根据传入的键值对的大小变化的。
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  // 要分别对每个char中的每位做判断
  uint32_t num = 0;

  // 先以char为单位
  size_t i_num = BUCKET_ARRAY_SIZE / 8;
  for (size_t i = 0; i < i_num; i++) {
    uint8_t c = static_cast<uint8_t>(readable_[i]);
    for (uint8_t j = 0; j < 8; j++) {
      // 取最低位判断
      if ((c & 1) > 0) {
        num++;
      }
      c >>= 1;
    }
  }

  // 最后还要看剩余的
  size_t i_remain = BUCKET_ARRAY_SIZE % 8;
  if (i_remain > 0) {
    uint8_t c = static_cast<uint8_t>(readable_[i_num]);
    for (size_t j = 0; j < i_remain; j++) {
      // 取最低位判断
      if ((c & 1) == 1) {
        num++;
      }
      c >>= 1;
    }
  }

  return num;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  for (int i = 0; i < static_cast<int>(BUCKET_ARRAY_SIZE); ++i) {
    if (IsReadable(i)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  // LOG_INFO("Bucket Capacity: %lu", BUCKET_ARRAY_SIZE);
  for (size_t bucket_idx = 0; bucket_idx < static_cast<size_t>(BUCKET_ARRAY_SIZE); bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;
    // LOG_DEBUG("*****************%d", (int)IsReadable(bucket_idx));
    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size（占用和被占用的总数）: %u, Taken(被占用的): %u, Free(一定所有的槽occupied): %u",
           BUCKET_ARRAY_SIZE, size, taken, free);
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
