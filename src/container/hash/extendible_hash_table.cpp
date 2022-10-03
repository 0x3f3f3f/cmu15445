//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/extendible_hash_table.h"
#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  this->directory_page_id_ = INVALID_PAGE_ID;
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  uint32_t hash_value = Hash(key);
  uint32_t res = dir_page->GetGlobalDepthMask() & hash_value;
  // res = dir_page->GetLocalDepthMask(res) & hash_value;
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  uint32_t res = dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  // 初始化目录的pageid
  if (directory_page_id_ == INVALID_PAGE_ID) {
    page_id_t directory_page_id;
    Page *directory_page = this->buffer_pool_manager_->NewPage(&directory_page_id);
    assert(directory_page != nullptr);

    HashTableDirectoryPage *res = reinterpret_cast<HashTableDirectoryPage *>(directory_page->GetData());
    res->SetPageId(directory_page_id);
    // 目录id要初始化
    this->directory_page_id_ = directory_page_id;
    page_id_t bucket_page_id;
    // 用不到第一个bukcet页的内容，但是要初始化他的bucket_page_id
    Page *bucket_page = this->buffer_pool_manager_->NewPage(&bucket_page_id);
    assert(bucket_page != nullptr);
    // 设置目录的内容
    res->SetLocalDepth(0, res->GetGlobalDepth());
    res->SetBucketPageId(0, bucket_page_id);
    // 获得完一定要释放，否则这个页会被一直占用，内存会越来越少，这里directory不用释放，因为本来就是返回要用的。
    // assert(buffer_pool_manager_->UnpinPage(directory_page_id, true));
    // 因为bucket_page内的数据没有任何更改
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));

    return res;
  }
  // 上面初始化的directory_page_id_后面直接获取
  Page *directory_page = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(directory_page != nullptr);
  // this->table_latch_.WUnlock();
  return reinterpret_cast<HashTableDirectoryPage *>(directory_page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  assert(page != nullptr);

  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  // this->table_latch_.WLock();
  HashTableDirectoryPage *directory = FetchDirectoryPage();
  page_id_t page_id = KeyToPageId(key, directory);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(page_id);

  bool res = bucket_page->GetValue(key, comparator_, result);
  // 这里获取值的时候，可能正好directory_id无效的时候
  assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
  // 只是读取值
  assert(buffer_pool_manager_->UnpinPage(page_id, false));
  // this->table_latch_.WUnlock();
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  // this->table_latch_.WLock();
  // 获取目录页
  HashTableDirectoryPage *directory = this->FetchDirectoryPage();
  // 获取第几个目录项
  uint32_t directory_idx = KeyToDirectoryIndex(key, directory);
  // 根据key映射成page_id
  page_id_t bucket_page_id = KeyToPageId(key, directory);
  // LOG_DEBUG("**************");
  // 根据page_id获得对应的页的
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  // LOG_DEBUG("++++++++++++++");
  bool flag = bucket->Insert(key, value, comparator_);
  // bucket->PrintBucket();
  if (!flag) {
    if (bucket->IsFull()) {
      if (!SplitInsert(transaction, key, value)) {
        assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
        assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
        // this->table_latch_.WUnlock();
        return false;
      }
    } else {
      // 可以进行优化
      assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
      assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
      // this->table_latch_.WUnlock();
      return false;
    }
  }
  // directory->PrintDirectory();
  // 插入成功，更新directory
  directory->SetBucketPageId(directory_idx, bucket_page_id);
  assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  // this->table_latch_.WUnlock();
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  HashTableDirectoryPage *directory = this->FetchDirectoryPage();
  uint32_t directory_idx = KeyToDirectoryIndex(key, directory);

  if (directory->GetLocalDepth(directory_idx) >= directory->GetGlobalDepth()) {
    // key出现极限多个相等的情况。
    if (directory->Size() >= DIRECTORY_ARRAY_SIZE) {
      assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
      return false;
    }
    directory->IncrGlobalDepth();
    // directory->PrintDirectory();
  }

  directory->IncrLocalDepth(directory_idx);
  // distance计算根据当前directory_idx增加ld以后的值计算，distance最少都是2，不用考虑为1的情况
  uint32_t distance = 1 << directory->GetLocalDepth(directory_idx);
  // LOG_DEBUG("%d************************", (int)distance);
  // LOG_DEBUG("%d************************", (int)directory_idx);
  // 把指向相同page_id的位置所有的page_id和ld修改成一样的
  for (uint32_t st = directory_idx; st >= 0; st -= distance) {
    directory->SetBucketPageId(st, directory->GetBucketPageId(directory_idx));
    directory->SetLocalDepth(st, directory->GetLocalDepth(directory_idx));
    // 因为uint32_t类型，无符号
    if (st < distance) {
      break;
    }
  }

  for (uint32_t st = directory_idx; st < directory->Size(); st += distance) {
    directory->SetBucketPageId(st, directory->GetBucketPageId(directory_idx));
    directory->SetLocalDepth(st, directory->GetLocalDepth(directory_idx));
  }
  // directory->PrintDirectory();

  // LOG_DEBUG("%d--------------------------------------------", (int)split_page_idx);
  // LOG_DEBUG("%d+++++++++++++++++++++++++++++++++", (int)split_page_idx);
  // 计算splitidx注意也是根据增加ld以后的值算的
  uint32_t split_page_idx = directory->GetSplitImageIndex(directory_idx);
  page_id_t split_page_id;
  Page *split_page_origin = this->buffer_pool_manager_->NewPage(&split_page_id);
  // LOG_DEBUG("%d---------------------------------------", (int)split_page_id);
  assert(split_page_origin != nullptr);
  HASH_TABLE_BUCKET_TYPE *split_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(split_page_origin);

  directory->SetBucketPageId(split_page_idx, split_page_id);
  // directory->bucket_page_ids_[split_page_idx] = split_page_id;
  directory->SetLocalDepth(split_page_idx, directory->GetLocalDepth(directory_idx));

  // 设置split——idx对立面的page_id和ld为新的内容
  for (uint32_t st = split_page_idx; st >= 0; st -= distance) {
    directory->SetBucketPageId(st, directory->GetBucketPageId(split_page_idx));
    directory->SetLocalDepth(st, directory->GetLocalDepth(split_page_idx));
    if (st < distance) {
      break;
    }
  }

  for (uint32_t st = split_page_idx; st < directory->Size(); st += distance) {
    directory->SetBucketPageId(st, directory->GetBucketPageId(split_page_idx));
    directory->SetLocalDepth(st, directory->GetLocalDepth(split_page_idx));
  }

  // directory->PrintDirectory();
  page_id_t target_page_id = directory->GetBucketPageId(directory_idx);
  Page *target_page_origin = this->buffer_pool_manager_->FetchPage(target_page_id);
  assert(target_page_origin != nullptr);
  HASH_TABLE_BUCKET_TYPE *target_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(target_page_origin);

  std::vector<MappingType> res;
  target_page->GetExistedData(&res);
  target_page->ResetData();
  for (uint32_t i = 0; i < res.size(); ++i) {
    uint32_t idx = KeyToDirectoryIndex(res[i].first, directory);
    if (idx == directory_idx) {
      target_page->Insert(res[i].first, res[i].second, comparator_);
    } else {
      split_page->Insert(res[i].first, res[i].second, comparator_);
    }
  }
  
  // target_page->PrintBucket();
  // split_page->PrintBucket();
  if (Insert(transaction, key, value)) {
    assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
    assert(buffer_pool_manager_->UnpinPage(target_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(split_page_id, true));
    return true;
  }
  assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
  assert(buffer_pool_manager_->UnpinPage(target_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(split_page_id, true));
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  // this->table_latch_.WLock();
  // 获取目录页
  HashTableDirectoryPage *directory = this->FetchDirectoryPage();
  // LOG_DEBUG("**************%d", directory_idx);
  // 根据key映射成page_id
  page_id_t bucket_page_id = KeyToPageId(key, directory);
  // LOG_DEBUG("**************");
  // 根据page_id获得对应的页的
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  // LOG_DEBUG("++++++++++++++++++++++++++++++++++++++++++++");
  bool flag = bucket->Remove(key, value, comparator_);
  // LOG_DEBUG("++++++++++++++++++++++++++++++++++++++++++++%d", flag);
  // 没有找到对应的key 和 value进行删除
  if (!flag) {
    // this->table_latch_.WUnlock();
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
    return false;
  }
  // 找到了判断这个bucket_id的页内容是否为空，如果空就需要进行合并操作
  if (flag && bucket->IsEmpty()) {
    Merge(transaction, key, value);
  }
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
  // this->table_latch_.WUnlock();
  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 获取目录页
  HashTableDirectoryPage *directory = this->FetchDirectoryPage();
  // key得到目录idx
  uint32_t directory_idx = KeyToDirectoryIndex(key, directory);
  // 用来删除bucket_page_id对应的页，因为这页已经置空了
  page_id_t bucket_page_id = KeyToPageId(key, directory);
  // 首先判断分裂的那部分LD还和当前为空的bucket LD是否相等， 不相等不能合并， LD必须大于0
  uint32_t split_idx = directory->GetSplitImageIndex(directory_idx);
  uint8_t current_ld = directory->GetLocalDepth(directory_idx);
  // 位置必须放在这里，放到后面会越界
  if (current_ld == 0) {
    assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
    return;
  }
  uint8_t split_ld = directory->GetLocalDepth(split_idx);
  if (split_ld != current_ld) {
    assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
    return;
  }
  // 只有真的合并的时候，才能删除这个bucket_page_id进行检验
  assert(buffer_pool_manager_->DeletePage(bucket_page_id));

  // 合并的时候，需要把目录中指向要删除的bucket的指针，指向splitImage,同时对目录的LD重置为0
  page_id_t target_page_id = directory->GetBucketPageId(directory_idx);
  page_id_t split_page_id = directory->GetBucketPageId(split_idx);
  directory->DecrLocalDepth(split_idx);
  split_ld = directory->GetLocalDepth(split_idx);
  for (uint32_t i = 0; i < directory->Size(); ++i) {
    // 还要设置split_page
    if (directory->GetBucketPageId(i) == target_page_id || directory->GetBucketPageId(i) == split_page_id) {
      directory->SetBucketPageId(i, split_page_id);
      directory->SetLocalDepth(i, split_ld);
    }
  }
  // 收缩可能重复收缩
  // directory的shrink， 本质就是GD的减小
  while (directory->CanShrink()) {
    directory->DecrGlobalDepth();
  }
  assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
